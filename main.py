"""
批量撤回插件 - 支持自动撤回、按数量批量撤回、按编号撤回、引用撤回等功能
编号规则：最新消息为1号，依次递增，记录所有群消息（包含群友和机器人）
指令回复消息（消息列表、撤回结果通知等）记录到历史但不自动撤回
用户发送的指令消息本身不记录到历史（避免编号偏移），可通过引用撤回
按编号撤回时，以该用户最后一次成功发送的消息列表为准，新消息不改变编号
支持任意命令前缀（/、.、!等），参数提取优先使用AstrBot框架解析后的消息段
"""

import asyncio
import re
import time
from functools import wraps
from typing import Optional

from aiocqhttp.exceptions import ActionFailed
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.message.components import At, Plain, Reply
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)
from astrbot.core.star.filter.command import CommandFilter
from astrbot.core.star.filter.command_group import CommandGroupFilter

from .confirmation import RecallConfirmations
from .delivery import (
    LISTING_KEY,
    NO_AUTO_RECALL,
    DeliveryObserver,
    Destination,
    EventDeliveryScope,
    Receipt,
    event_destination,
)
from .listings import Listings, PendingListing


def _finish_command(handler):
    """禁用已匹配指令的默认 LLM 回退，并在回复发送后终止事件。"""

    @wraps(handler)
    async def wrapped(self, event, *args, **kwargs):
        # AstrBot 的这个参数实际表示“禁止默认 LLM 请求”，True 才是禁用。
        # 此标记独立于 result，旧版框架 clear_result() 后仍然有效。
        event.should_call_llm(True)
        event.set_extra(NO_AUTO_RECALL, True)
        try:
            async for result in handler(self, event, *args, **kwargs):
                yield result
        finally:
            event.stop_event()

    return wrapped


class BatchRecall(Star):
    """批量撤回插件主类"""

    _RECALL_TIMEOUT_ERROR = "撤回请求超时，协议端未及时响应，请稍后重试"
    _RECALL_UNCONFIRMED_ERROR = (
        "撤回接口已接受请求，但未收到撤回确认，请检查协议端日志或稍后重试"
    )

    def __init__(self, context: Context, config):
        super().__init__(context)
        self.conf = config
        self.context = context
        self.recall_tasks = set()
        # 消息历史记录：{session_id: [(message_id, content_preview, timestamp, sender_id, sender_name, is_bot), ...]}
        # 最新消息在列表最前面，编号从1开始
        self.message_history: dict[str, list[tuple[int, str, int, str, str, bool]]] = {}
        self.max_history = self.conf.get("max_message_history", 50)
        self.listings = Listings()
        self.confirmations = RecallConfirmations()
        self.delivery_scope = EventDeliveryScope()
        self.observers: dict[int, DeliveryObserver] = {}
        self.stopping = False
        logger.info(
            f"批量撤回插件已加载，自动撤回时间: {self.conf['recall_time']}秒，消息历史记录: {self.max_history}条"
        )

    def _get_session_id(self, event) -> str:
        """按平台实例和消息类型隔离历史，独立于 LLM 会话隔离设置。"""
        return event_destination(event).key

    def _add_message_to_history(
        self,
        session_id: str,
        message_id: int,
        content: str,
        sender_id: str,
        sender_name: str,
        is_bot: bool = False,
        msg_time: Optional[int] = None,
    ):
        """添加消息到历史记录，保持最新消息在最前面（编号1为最新）"""
        if session_id not in self.message_history:
            self.message_history[session_id] = []

        if msg_time is None:
            msg_time = int(time.time())

        # 检查是否已存在此消息ID（避免重复）
        for existing in self.message_history[session_id]:
            if existing[0] == message_id:
                return

        # 插入到最前面（最新消息编号为1）
        self.message_history[session_id].insert(
            0, (message_id, content[:100], msg_time, sender_id, sender_name, is_bot)
        )

        # 限制历史记录长度
        if len(self.message_history[session_id]) > self.max_history:
            self.message_history[session_id] = self.message_history[session_id][
                : self.max_history
            ]

    def _remove_message_from_history(self, session_id: str, message_id: int):
        """从历史记录中移除已撤回的消息"""
        if session_id in self.message_history:
            self.message_history[session_id] = [
                msg for msg in self.message_history[session_id] if msg[0] != message_id
            ]

    def _extract_message_content_from_segments(self, segments: list) -> str:
        """从OneBot消息段列表中提取纯文本内容预览"""
        if isinstance(segments, str):
            return segments[:50]
        content_parts = []
        for msg in segments:
            if isinstance(msg, dict):
                msg_type = msg.get("type")
                if msg_type == "text":
                    text = msg.get("data", {}).get("text", "").strip()
                    if text:
                        content_parts.append(text)
                elif msg_type == "image":
                    content_parts.append("[图片]")
                elif msg_type == "face":
                    content_parts.append("[表情]")
                elif msg_type == "at":
                    content_parts.append("[@]")
                elif msg_type == "reply":
                    content_parts.append("[引用]")
                elif msg_type == "record":
                    content_parts.append("[语音]")
                elif msg_type in {"video", "file", "forward"}:
                    content_parts.append(f"[{msg_type}]")
        content = "".join(content_parts)
        return content[:50] if content else "[无文本内容]"

    def _remove_task(self, task: asyncio.Task):
        """移除已完成的任务"""
        self.recall_tasks.discard(task)

    def _mark_no_auto_recall(self, event):
        """只豁免本事件的指令回执，不影响其他群或并发任务。"""
        event.set_extra(NO_AUTO_RECALL, True)

    def _strip_command_prefix(self, text: str) -> str:
        """剥离开头的命令前缀符号（/、.、!、#等，包括中英文全半角符号），返回去除前缀后的文本"""
        return text.lstrip(
            "/.!#$%^&*~-+=?，。、！!＠@＃#＄$％%＾^＆&＊*～~｀`｜|＼\\ 　\t"
        )

    def _extract_command_tail(self, raw_text: str, cmd_names: tuple[str, ...]) -> str:
        """
        从原始文本中提取命令名之后的参数部分。
        raw_text: 用户发送的原始文本（可能包含命令前缀如/.!等）
        cmd_names: 可能的命令名元组，按长度降序排列以优先匹配长命令名
        返回命令名之后的参数文本（已strip），如果不是本命令返回空字符串
        """
        if not raw_text:
            return ""
        stripped = self._strip_command_prefix(raw_text)
        if not stripped:
            return ""
        # 按长度降序排列，避免短命令名先匹配（如"撤回"先于"撤回自身"）
        sorted_cmds = sorted(cmd_names, key=len, reverse=True)
        for cmd in sorted_cmds:
            if stripped.startswith(cmd):
                return stripped[len(cmd) :].strip()
        return ""

    def _extract_text_from_plain_segments(self, event) -> str:
        """从消息段的 Plain 组件中提取并拼接纯文本"""
        plain_parts: list[str] = []
        for segment in event.get_messages():
            if isinstance(segment, Plain):
                plain_parts.append(segment.text)
        return "".join(plain_parts).strip()

    def _is_plugin_command(self, text: str) -> bool:
        """
        判断文本是否是本插件的指令消息（用户发送的）
        指令消息不记录到历史，避免编号偏移
        支持任意命令前缀（/、.、!、#等），通过剥离开头所有非字母数字字符来识别
        """
        t = text.strip()
        if not t:
            return False
        stripped = self._strip_command_prefix(t)
        if not stripped:
            return False
        # 精确匹配指令前缀（指令词后必须是空格、数字、逗号、@或结尾，避免误过滤普通聊天）
        cmd_prefixes = (
            "批量撤回",
            "消息列表",
            "撤回自身",
            "test_recall",
            "recall_config",
        )
        for prefix in cmd_prefixes:
            if stripped.startswith(prefix):
                after = stripped[len(prefix) :]
                if after == "" or after[0] in (
                    " ",
                    "\t",
                    ",",
                    "，",
                    "@",
                    "0",
                    "1",
                    "2",
                    "3",
                    "4",
                    "5",
                    "6",
                    "7",
                    "8",
                    "9",
                ):
                    return True
        # 单独的"撤回"指令（用于引用撤回，精确匹配）
        if stripped in ("撤回",):
            return True
        return False

    async def _recall_msg(
        self, client, message_id: int, session_id: Optional[str] = None
    ):
        """自动撤回消息"""
        recall_time = self.conf["recall_time"]
        logger.info(f"⏰ 等待 {recall_time} 秒后撤回消息 {message_id}")

        await asyncio.sleep(recall_time)
        if message_id:
            ok, _ = await self._do_recall(client, message_id)
            if ok and session_id:
                self._remove_message_from_history(session_id, message_id)

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    async def on_message(self, event: AstrMessageEvent):
        """监听所有收到的消息，记录到历史中；同时处理引用撤回的fallback"""
        try:
            if not isinstance(event, AiocqhttpMessageEvent):
                return

            raw_event = getattr(event.message_obj, "raw_message", None)
            if not raw_event or raw_event.get("post_type") != "message":
                return

            # 使用框架已经匹配的指令，包含其他插件的指令、别名和子指令。
            # 不终止事件，让对应插件正常执行；显式 request_llm 仍由框架处理。
            # 普通聊天/未匹配的前缀消息不受影响。
            if any(
                isinstance(f, (CommandFilter, CommandGroupFilter))
                for handler in event.get_extra("activated_handlers", [])
                for f in handler.event_filters
            ):
                event.should_call_llm(True)
                self._mark_no_auto_recall(event)

            # 只记录群消息
            if not event.get_group_id():
                return

            # ===== 引用撤回 fallback：当 @filter.command("撤回") 无法匹配带Reply段的消息时 =====
            # 检测消息是否包含 Reply 段 + 文本为"撤回"（可能带命令前缀）
            messages = event.get_messages()
            reply_id = None
            has_recall_text = (
                self._strip_command_prefix(
                    self._extract_text_from_plain_segments(event)
                )
                == "撤回"
            )
            for segment in messages:
                if isinstance(segment, Reply):
                    reply_id = int(segment.id)
                    break

            if reply_id and has_recall_text:
                event.should_call_llm(True)
                logger.info(
                    f"[BatchRecall] source=reply_fallback message_id={reply_id}"
                )
                # 权限检查：优先用 event.is_admin()，回退用 raw_event 中的 role 字段
                is_admin = False
                if hasattr(event, "is_admin"):
                    try:
                        is_admin = event.is_admin()
                    except Exception:
                        pass
                if not is_admin:
                    # 回退：从 raw_event 的 sender.role 判断
                    if raw_event:
                        role = raw_event.get("sender", {}).get("role", "")
                        is_admin = role in ("owner", "admin")
                if not is_admin:
                    self._mark_no_auto_recall(event)
                    yield event.plain_result("撤回命令仅管理员可使用。")
                    event.stop_event()
                    return
                async for result in self._recall_reply(event, reply_id):
                    yield result
                event.stop_event()
                return
            # ===== 引用撤回 fallback 结束 =====

            session_id = self._get_session_id(event)
            if not raw_event:
                return

            message_id = raw_event.get("message_id")
            if not message_id:
                return

            # 提取消息纯文本
            raw_message = raw_event.get("message", [])
            content = self._extract_message_content_from_segments(raw_message)

            sender_id = str(raw_event.get("sender", {}).get("user_id", ""))
            bot_self_id = str(event.get_self_id())
            is_bot = sender_id == bot_self_id

            # 用户发的插件指令消息不记录（避免编号偏移）
            # 机器人发送记录由平台回执写入；协议端自身消息事件通过消息 ID 去重
            if not is_bot and self._is_plugin_command(content):
                return

            sender_name = raw_event.get("sender", {}).get("card") or raw_event.get(
                "sender", {}
            ).get("nickname", "未知")
            msg_time = raw_event.get("time", int(time.time()))

            self._add_message_to_history(
                session_id,
                int(message_id),
                content,
                sender_id,
                sender_name,
                is_bot,
                int(msg_time),
            )
        except Exception as e:
            logger.error(f"记录消息到历史失败: {e}")

    async def initialize(self):
        self.delivery_scope.install()
        self._attach_clients()

    @filter.on_platform_loaded()
    async def on_platform_loaded(self):
        self._attach_clients()

    def _destination_allowed(self, destination: Destination) -> bool:
        if self.context is None:
            return True
        plugins = self.context.get_config(destination.umo).get("plugin_set", ["*"])
        return plugins == ["*"] or "astrbot_plugin_batchrecall" in plugins

    def _attach_clients(self):
        if self.stopping or self.context is None:
            return
        for platform in self.context.platform_manager.platform_insts:
            if platform.meta().name != "aiocqhttp":
                continue
            bot = platform.bot
            if id(bot) in self.observers:
                continue
            platform_id = platform.meta().id

            async def received(receipt, bot=bot):
                await self._received(bot, receipt)

            async def recalled(notice, bot=bot, platform_id=platform_id):
                self._recalled(bot, platform_id, notice)

            observer = DeliveryObserver(
                bot, platform_id, received, recalled, self._destination_allowed, logger
            )
            observer.install()
            self.observers[id(bot)] = observer

    async def _received(self, bot, receipt: Receipt):
        destination = receipt.destination
        event = receipt.event
        bot_id = ""
        bot_name = "机器人"
        if event is not None:
            bot_id = str(event.get_self_id())
            if hasattr(event, "get_self_name"):
                bot_name = event.get_self_name()
        preview = self._extract_message_content_from_segments(receipt.message)
        self._add_message_to_history(
            destination.key, receipt.message_id, preview, bot_id, bot_name, is_bot=True
        )
        if event is not None:
            pending = event.get_extra(LISTING_KEY)
            if isinstance(pending, PendingListing):
                self.listings.remember(destination.key, pending, receipt.message_id)
                event.set_extra(LISTING_KEY, None)
            if event.get_extra(NO_AUTO_RECALL, False):
                return
        if self._should_recall_destination(destination, event):
            task = asyncio.create_task(
                self._recall_msg(bot, receipt.message_id, destination.key)
            )
            task.add_done_callback(self._remove_task)
            self.recall_tasks.add(task)

    def _should_recall_destination(self, destination: Destination, event) -> bool:
        if destination.is_group:
            groups = self.conf.get("group_whitelist", [])
            return self.conf.get("enable_group_recall", True) and (
                not groups or destination.target_id in groups
            )
        is_admin = event is not None and event.is_admin()
        if event is None and self.context is not None:
            admins = self.context.get_config(destination.umo).get("admins_id", [])
            is_admin = destination.target_id in admins
        if is_admin:
            return self.conf.get("enable_admin_private_recall", False)
        return self.conf.get("enable_private_recall", True)

    def _recalled(self, bot, platform_id: str, notice: dict):
        message_id = int(notice["message_id"])
        self.confirmations.confirm(bot, message_id)
        group = str(notice.get("group_id") or "")
        target = group or str(notice["user_id"])
        destination = Destination(platform_id, target, bool(group))
        self._remove_message_from_history(destination.key, message_id)
        logger.info(
            f"[BatchRecall] target={destination.key} message_id={message_id} "
            "reason=recall_notice_confirmed"
        )

    async def _call_api(self, request, action: str):
        """限制本插件的协议请求等待时间，不修改共享 OneBot 客户端配置。"""
        timeout = self.conf.get("api_timeout", 10)
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            timeout = 10
        try:
            return await asyncio.wait_for(request, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"OneBot API 超时: {action}, 等待上限 {timeout} 秒")
            raise

    async def _do_recall(self, bot, message_id: int) -> tuple[bool, str]:
        """注册确认再发请求；接口接受与实际撤回分别记录。"""
        try:
            with self.confirmations.track(bot, message_id) as confirmation:
                started = time.monotonic()
                await self._call_api(
                    bot.delete_msg(message_id=message_id),
                    f"delete_msg message_id={message_id}",
                )
                logger.info(
                    f"[BatchRecall] message_id={message_id} reason=recall_api_accepted"
                )
                try:
                    await asyncio.wait_for(
                        asyncio.shield(confirmation),
                        timeout=self.conf.get("confirmation_timeout", 2),
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"[BatchRecall] message_id={message_id} "
                        "reason=recall_unconfirmed history=preserved"
                    )
                    return False, self._RECALL_UNCONFIRMED_ERROR
                logger.info(
                    f"[BatchRecall] message_id={message_id} reason=recall_confirmed "
                    f"elapsed_ms={round((time.monotonic() - started) * 1000)}"
                )
            return True, ""
        except asyncio.TimeoutError:
            return False, self._RECALL_TIMEOUT_ERROR
        except ActionFailed as e:
            retcode = getattr(e, "retcode", None)
            detail = ""
            result = getattr(e, "result", None)
            if isinstance(result, dict):
                detail = str(result.get("wording") or result.get("message") or "")[:300]
            logger.warning(
                f"[BatchRecall] message_id={message_id} reason=recall_failed "
                f"retcode={retcode} detail={detail}"
            )
            if retcode == 1200:
                return False, "消息已撤回或超时"
            return False, f"撤回失败(retcode={retcode})"
        except Exception as e:
            logger.warning(f"撤回消息失败: message_id={message_id}, {e}")
            return False, f"撤回失败: {str(e)}"

    @_finish_command
    async def _recall_reply(self, event: AiocqhttpMessageEvent, reply_id: int):
        """三个引用撤回入口共用处理结果，避免旧框架清除 STOP 后重复撤回。"""
        if event.get_extra("batchrecall_reply_handled", False):
            return
        # 在第一次 await 前占用当前事件；标记不与其他消息或群聊共享。
        event.set_extra("batchrecall_reply_handled", True)
        ok, err = await self._do_recall(event.bot, reply_id)
        if ok:
            self._remove_message_from_history(self._get_session_id(event), reply_id)
        if self.conf.get("enable_recall_notification", True):
            self._mark_no_auto_recall(event)
            if ok:
                yield event.plain_result("✅ 已撤回引用的消息。")
            else:
                text = f"❌ {err}"
                if err not in {
                    self._RECALL_TIMEOUT_ERROR,
                    self._RECALL_UNCONFIRMED_ERROR,
                }:
                    text += "\n提示：机器人需要管理员权限才能撤回他人消息。"
                yield event.plain_result(text)

    @filter.command("test_recall")
    @_finish_command
    async def test_recall_command(self, event: AstrMessageEvent):
        """测试撤回功能（此测试消息会被记录并自动撤回，不标记为指令回复）"""
        recall_time = self.conf["recall_time"]
        event.set_extra(NO_AUTO_RECALL, False)
        yield event.plain_result(f"🧪 测试消息，{recall_time}秒后此消息将会撤回...")

    @filter.command("recall_config")
    @_finish_command
    async def recall_config_command(self, event: AstrMessageEvent):
        """查看当前配置"""
        self._mark_no_auto_recall(event)
        config_info = "📋 当前撤回配置:\n"
        config_info += f"撤回时间: {self.conf['recall_time']}秒\n"
        config_info += f"协议请求超时: {self.conf.get('api_timeout', 10)}秒\n"
        config_info += f"私聊启用: {self.conf.get('enable_private_recall', True)}\n"
        config_info += f"群聊启用: {self.conf.get('enable_group_recall', True)}\n"
        config_info += (
            f"撤回结果通知: {self.conf.get('enable_recall_notification', True)}\n"
        )
        config_info += f"消息历史记录: {self.conf.get('max_message_history', 50)}条\n"
        group_whitelist = self.conf.get("group_whitelist", [])
        if group_whitelist:
            config_info += f"白名单群: {len(group_whitelist)}个\n"
        else:
            config_info += "白名单群: 所有群聊\n"
        yield event.plain_result(config_info)

    async def terminate(self):
        """插件卸载时取消所有撤回任务"""
        self.stopping = True
        for observer in self.observers.values():
            observer.uninstall()
        self.observers.clear()
        self.delivery_scope.uninstall()
        self.confirmations.close()
        for task in self.recall_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.recall_tasks, return_exceptions=True)
        self.recall_tasks.clear()
        logger.info("批量撤回插件已卸载")

    async def _load_history_from_api(
        self, event: AiocqhttpMessageEvent, session_id: str
    ):
        """从API加载历史消息（当本地历史为空时使用）"""
        is_group = bool(event.get_group_id())
        if not is_group:
            return

        try:
            fetch_count = min(self.max_history * 2, 100)
            payloads = {
                "group_id": int(session_id),
                "count": fetch_count,
            }
            result = await self._call_api(
                event.bot.call_action("get_group_msg_history", **payloads),
                "get_group_msg_history",
            )
            history_messages = (
                result.get("messages", []) if isinstance(result, dict) else []
            )

            # 按时间倒序排列（最新在前）
            history_messages.sort(key=lambda item: item.get("time", 0), reverse=True)

            bot_self_id = str(event.get_self_id())
            history_list = []
            for msg in history_messages[: self.max_history]:
                msg_id = msg.get("message_id")
                if not msg_id:
                    continue
                sender_info = msg.get("sender", {})
                sender_id = str(sender_info.get("user_id", ""))
                # 跳过API返回的历史中用户发送的指令消息（避免把历史中的"批量撤回"等指令也编号）
                raw_msg = msg.get("message", [])
                if isinstance(raw_msg, list):
                    content = self._extract_message_content_from_segments(raw_msg)
                    if sender_id != bot_self_id and self._is_plugin_command(content):
                        continue
                else:
                    content = str(raw_msg)[:50]

                sender_name = sender_info.get("card") or sender_info.get(
                    "nickname", "未知"
                )
                msg_time = msg.get("time", int(time.time()))
                is_bot = sender_id == bot_self_id

                history_list.append(
                    (
                        int(msg_id),
                        content,
                        int(msg_time),
                        sender_id,
                        sender_name,
                        is_bot,
                    )
                )

            self.message_history[session_id] = history_list
        except Exception as exc:
            logger.error(f"从API获取消息历史失败: {exc}")

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("消息列表")
    @_finish_command
    async def message_list_command(self, event: AstrMessageEvent):
        """
        显示最近消息列表:消息列表 [显示数量]
        包含所有群成员的消息，最新消息编号为1
        本消息列表自身将占据编号1，历史消息从编号2开始
        """
        if not isinstance(event, AiocqhttpMessageEvent):
            self._mark_no_auto_recall(event)
            yield event.plain_result("当前平台不支持此功能，仅支持 QQ 协议端。")
            return

        session_id = self._get_session_id(event)
        is_group = bool(event.get_group_id())
        if not is_group:
            self._mark_no_auto_recall(event)
            yield event.plain_result("当前仅支持群聊中查看消息列表。")
            return

        # 获取当前指令消息的ID，用于排除（用户发的指令消息不编号）
        current_msg_id = None
        raw_event = getattr(event.message_obj, "raw_message", None)
        if raw_event:
            current_msg_id = raw_event.get("message_id")

        # 参数提取：从 Plain 消息段拼接文本，回退使用 message_str
        plain_parts: list[str] = []
        for segment in event.get_messages():
            if isinstance(segment, Plain):
                plain_parts.append(segment.text)
        raw_text_from_plain = "".join(plain_parts).strip()
        raw_text_from_msg_str = event.get_message_str().strip()

        raw_text = raw_text_from_plain or raw_text_from_msg_str

        tail = self._extract_command_tail(raw_text, ("消息列表",))

        nums = re.findall(r"\d+", tail)
        show_count = int(nums[0]) if nums else 10
        # 预留1个位置给本消息列表自身
        show_count = min(show_count, self.max_history - 1)

        # 加载历史，排除当前用户指令消息
        history = self.message_history.get(session_id, [])
        if not history:
            await self._load_history_from_api(event, session_id)
            history = self.message_history.get(session_id, [])
        if current_msg_id and history:
            history = [m for m in history if m[0] != int(current_msg_id)]

        if not history:
            self._mark_no_auto_recall(event)
            yield event.plain_result("当前没有可显示的消息记录。请先发几条消息后再试。")
            return

        # 历史消息从编号2开始显示（编号1预留给本消息列表）
        display_msgs = history[:show_count]
        total = len(history) + 1  # +1是本消息列表自身
        list_text = f"📋 最近消息 (共{total}条，显示{len(display_msgs) + 1}条):\n"
        list_text += "格式: [编号] 发送者: 内容\n"
        list_text += "─" * 25 + "\n"

        # 编号1是本消息列表自身
        bot_name = (
            event.get_self_name() if hasattr(event, "get_self_name") else "机器人"
        )
        list_text += f"[1] 🤖 {bot_name}: (本消息列表)\n"

        # 历史消息从编号2开始
        for offset, (
            msg_id,
            content,
            msg_time,
            sender_id,
            sender_name,
            is_bot,
        ) in enumerate(display_msgs, 2):
            name_prefix = "🤖 " if is_bot else ""
            display_name = (
                sender_name[:8] + ".." if len(sender_name) > 8 else sender_name
            )
            list_text += f"[{offset}] {name_prefix}{display_name}: {content}\n"

        list_text += "─" * 25 + "\n"
        list_text += "💡 使用方法:\n"
        list_text += "• 批量撤回 1 3 5 - 撤回指定编号消息\n"
        list_text += "• 批量撤回 1,3,5 - 逗号分隔也支持\n"
        list_text += "• 引用消息+撤回 - 撤回被引用的消息\n"
        list_text += "• 批量撤回 5 - 撤回最近5条消息(兼容旧版)\n"
        list_text += "• 撤回自身 5 - 仅撤回机器人最近5条"

        self._mark_no_auto_recall(event)
        event.set_extra(
            LISTING_KEY,
            PendingListing(
                str(event.get_sender_id()),
                tuple(display_msgs),
                str(event.get_self_id()),
                bot_name,
            ),
        )
        yield event.plain_result(list_text)
        event.stop_event()

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("撤回自身")
    @_finish_command
    async def recall_bot_messages_command(self, event: AstrMessageEvent):
        """
        撤回机器人自身发送的消息:撤回自身 撤回数量
        """
        if not isinstance(event, AiocqhttpMessageEvent):
            self._mark_no_auto_recall(event)
            yield event.plain_result("当前平台不支持撤回自身消息，仅支持 QQ 协议端。")
            return

        group_id = event.get_group_id()
        if not group_id:
            self._mark_no_auto_recall(event)
            yield event.plain_result("当前仅支持群聊中撤回机器人自身消息。")
            return

        # 获取当前指令消息ID
        current_msg_id = None
        raw_event_local = getattr(event.message_obj, "raw_message", None)
        if raw_event_local:
            current_msg_id = raw_event_local.get("message_id")

        # 参数提取：从 Plain 消息段拼接文本，回退使用 message_str
        plain_parts: list[str] = []
        for segment in event.get_messages():
            if isinstance(segment, Plain):
                plain_parts.append(segment.text)
        raw_text_from_plain = "".join(plain_parts).strip()
        raw_text_from_msg_str = event.get_message_str().strip()

        raw_text = raw_text_from_plain or raw_text_from_msg_str

        tail = self._extract_command_tail(raw_text, ("撤回自身",))

        if not re.fullmatch(r"[0-9]+", tail):
            self._mark_no_auto_recall(event)
            yield event.plain_result("撤回数量必须为正整数，例如：撤回自身 5")
            return
        nums = [tail]
        if not nums:
            self._mark_no_auto_recall(event)
            yield event.plain_result("请在指令后填写需要撤回的数量，例如：撤回自身 5")
            return

        count = int(nums[-1])
        if count <= 0:
            self._mark_no_auto_recall(event)
            yield event.plain_result("撤回数量必须为正整数。")
            return

        max_count = int(self.conf.get("batch_max_count", 20))
        if count > max_count:
            count = max_count

        session_id = self._get_session_id(event)

        # 获取发送指令前的消息快照（优先使用本地历史中的机器人消息）
        history = self.message_history.get(session_id, [])
        if not history:
            await self._load_history_from_api(event, session_id)
            history = self.message_history.get(session_id, [])
        # 创建快照，避免并发修改影响
        if current_msg_id and history:
            history = [m for m in history if m[0] != int(current_msg_id)]
        else:
            history = list(history)

        bot_messages = [msg for msg in history if msg[5]]  # is_bot=True

        if not bot_messages:
            try:
                fetch_count = min(max_count * 3, 100)
                payloads = {
                    "group_id": int(group_id),
                    "count": fetch_count,
                }
                result = await self._call_api(
                    event.bot.call_action("get_group_msg_history", **payloads),
                    "get_group_msg_history",
                )
                history_messages = (
                    result.get("messages", []) if isinstance(result, dict) else []
                )
                bot_self_id = str(event.get_self_id())
                bot_messages_raw = [
                    msg
                    for msg in history_messages
                    if str(msg.get("sender", {}).get("user_id", "")) == bot_self_id
                ]
                bot_messages_raw.sort(
                    key=lambda item: item.get("time", 0), reverse=True
                )
                bot_messages = []
                for msg in bot_messages_raw:
                    msg_id = msg.get("message_id")
                    if msg_id:
                        bot_messages.append(
                            (int(msg_id), "", 0, bot_self_id, "机器人", True)
                        )
            except Exception as exc:
                logger.error(f"撤回自身获取消息历史失败: {exc}")
                self._mark_no_auto_recall(event)
                yield event.plain_result("获取消息历史失败，无法执行撤回。")
                return

        if not bot_messages:
            if self.conf.get("enable_recall_notification", True):
                self._mark_no_auto_recall(event)
                yield event.plain_result("未找到可撤回的机器人消息。")
            event.stop_event()
            return

        success = 0
        failed_msgs = []
        for msg in bot_messages[:count]:
            message_id = msg[0]
            if not message_id:
                continue
            ok, err = await self._do_recall(event.bot, message_id)
            if ok:
                success += 1
                self._remove_message_from_history(session_id, message_id)
            else:
                failed_msgs.append(f"{message_id}({err})")
                if err in {self._RECALL_TIMEOUT_ERROR, self._RECALL_UNCONFIRMED_ERROR}:
                    break

        if self.conf.get("enable_recall_notification", True):
            result_text = f"已尝试撤回机器人最近 {success} 条消息。"
            if failed_msgs:
                result_text += f"\n失败: {', '.join(failed_msgs[:5])}"
            self._mark_no_auto_recall(event)
            yield event.plain_result(result_text)
        event.stop_event()

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("批量撤回")
    @_finish_command
    async def batch_recall_command(self, event: AstrMessageEvent):
        """
        批量撤回消息:
        - 批量撤回 1 3 4 9 (按编号撤回指定消息)
        - 批量撤回 1,3,4,9 (逗号分隔也支持)
        - 批量撤回 @用户 1 3 (撤回指定用户的指定编号消息)
        - 批量撤回 5 (撤回最近5条消息，兼容旧版)
        - 引用消息+批量撤回 (撤回被引用的消息)
        """
        if not isinstance(event, AiocqhttpMessageEvent):
            self._mark_no_auto_recall(event)
            yield event.plain_result("当前平台不支持批量撤回，仅支持 QQ 协议端。")
            return

        session_id = self._get_session_id(event)
        is_group = bool(event.get_group_id())
        if not is_group:
            self._mark_no_auto_recall(event)
            yield event.plain_result("当前仅支持群聊批量撤回。")
            return

        messages = event.get_messages()
        target_qq = None
        reply_id = None
        current_msg_id = None

        # 获取当前指令消息的原始ID
        raw_event = getattr(event.message_obj, "raw_message", None)
        if raw_event:
            current_msg_id = raw_event.get("message_id")

        # 从消息段中提取@和引用（这些结构化数据从event.get_messages()获取最可靠）
        for segment in messages:
            if isinstance(segment, At) and str(segment.qq) != "all":
                target_qq = str(segment.qq)
            elif isinstance(segment, Reply):
                reply_id = int(segment.id)

        # 参数提取：从 Plain 消息段拼接文本，回退使用 message_str
        plain_parts: list[str] = []
        for segment in messages:
            if isinstance(segment, Plain):
                plain_parts.append(segment.text)
        raw_text_from_plain = "".join(plain_parts).strip()
        raw_text_from_msg_str = event.get_message_str().strip()

        logger.debug(f"[批量撤回] Plain segments ({len(plain_parts)}): {plain_parts!r}")
        logger.debug(f"[批量撤回] message_str: {raw_text_from_msg_str!r}")

        raw_text = raw_text_from_plain or raw_text_from_msg_str

        logger.debug(f"[批量撤回] 使用的raw_text: {raw_text!r}")

        # 剥离开头的命令前缀符号和命令名
        tail = self._extract_command_tail(raw_text, ("批量撤回",))
        logger.debug(f"[批量撤回] 提取的参数tail: {tail!r}")

        # 直接用正则提取所有数字
        all_numbers = [int(x) for x in re.findall(r"\d+", tail)]
        logger.debug(f"[批量撤回] 提取到的数字: {all_numbers!r}")

        # 有引用消息时优先处理引用撤回
        if reply_id:
            async for result in self._recall_reply(event, reply_id):
                yield result
            event.stop_event()
            return

        if tail and not re.fullmatch(r"[0-9,，\s]+", tail):
            self._mark_no_auto_recall(event)
            yield event.plain_result(
                "数量和编号必须为正整数，多个编号用空格或逗号分隔。"
            )
            return

        if not all_numbers:
            self._mark_no_auto_recall(event)
            yield event.plain_result(
                "使用方法:\n"
                "• 批量撤回 1 3 5 - 按编号撤回指定消息\n"
                "• 批量撤回 1,3,5 - 逗号分隔也支持\n"
                "• 批量撤回 @用户 1 3 - 撤回指定用户的消息\n"
                "• 批量撤回 5 - 撤回最近5条消息\n"
                "• 引用消息+批量撤回 - 撤回被引用的消息\n"
                "发送「消息列表」可查看消息编号"
            )
            return

        max_count = int(self.conf.get("batch_max_count", 20))

        # 辅助函数：获取发送撤回指令之前的消息快照（排除当前用户指令消息）
        # 返回的是一个全新列表副本，后续即使有新消息进来也不会影响此快照的索引
        async def get_history_snapshot():
            hist = self.message_history.get(session_id, [])
            if not hist:
                await self._load_history_from_api(event, session_id)
                hist = self.message_history.get(session_id, [])
            # 始终创建浅拷贝，避免返回self.message_history的引用导致并发修改
            if current_msg_id and hist:
                snapshot = [m for m in hist if m[0] != int(current_msg_id)]
            else:
                snapshot = list(hist)
            return snapshot

        # 判断模式：
        # - 参数中有逗号 -> 按编号撤回
        # - 数字个数 >= 2 -> 按编号撤回（无论空格还是逗号分隔，多个数字就是多个编号）
        # - 参数中有空格分隔（说明有多个参数段） -> 按编号撤回
        # - 只有1个数字，无逗号无空格，无@ -> 按数量撤回（兼容旧版，撤回最近N条）
        has_comma = ("," in tail) or ("，" in tail)
        has_space_sep = bool(
            re.search(r"\d+\s+\d+", tail)
        )  # 数字之间有空白分隔 -> 多个编号
        is_by_count = (
            len(all_numbers) == 1
            and not has_comma
            and not has_space_sep
            and not target_qq
        )

        logger.debug(
            f"[批量撤回] 模式判断: len(all_numbers)={len(all_numbers)}, has_comma={has_comma}, has_space_sep={has_space_sep}, target_qq={target_qq}"
        )
        logger.debug(f"[批量撤回] is_by_count={is_by_count}")

        if is_by_count:
            logger.debug("[批量撤回] 进入按数量撤回模式")
            # 按数量撤回模式（兼容旧版）：可以重新加载以获取更多历史
            count = all_numbers[0]
            if count <= 0:
                self._mark_no_auto_recall(event)
                yield event.plain_result("撤回数量必须为正整数。")
                return
            if count > max_count:
                count = max_count

            history = await get_history_snapshot()

            if target_qq:
                filtered_messages = [msg for msg in history if msg[3] == str(target_qq)]
            else:
                filtered_messages = history

            if len(filtered_messages) < count:
                await self._load_history_from_api(event, session_id)
                history = await get_history_snapshot()
                if target_qq:
                    filtered_messages = [
                        msg for msg in history if msg[3] == str(target_qq)
                    ]
                else:
                    filtered_messages = history

            if not filtered_messages:
                if self.conf.get("enable_recall_notification", True):
                    self._mark_no_auto_recall(event)
                    yield event.plain_result("未找到可撤回的消息。")
                event.stop_event()
                return

            success = 0
            failed_msgs = []
            for msg in filtered_messages[:count]:
                message_id = msg[0]
                if not message_id:
                    continue
                ok, err = await self._do_recall(event.bot, message_id)
                if ok:
                    success += 1
                    self._remove_message_from_history(session_id, message_id)
                else:
                    failed_msgs.append(f"{message_id}({err})")
                    if err in {
                        self._RECALL_TIMEOUT_ERROR,
                        self._RECALL_UNCONFIRMED_ERROR,
                    }:
                        break

            if self.conf.get("enable_recall_notification", True):
                result_text = f"已尝试撤回最近 {success} 条消息。"
                if failed_msgs:
                    result_text += f"\n失败: {', '.join(failed_msgs[:5])}"
                self._mark_no_auto_recall(event)
                yield event.plain_result(result_text)
            event.stop_event()
            return

        else:
            logger.debug(
                f"[批量撤回] 进入按编号撤回模式, numbers={sorted(list(set(all_numbers)))}"
            )
            # 绑定该用户最后一次成功发送的消息列表，不按当前历史重新编号。
            numbers = sorted(list(set(all_numbers)))  # 去重并排序
            if len(numbers) > max_count:
                numbers = numbers[:max_count]

            displayed = self.listings.get(session_id, str(event.get_sender_id()))
            if displayed is None:
                self._mark_no_auto_recall(event)
                yield event.plain_result("请先发送「消息列表」，再按该列表的编号撤回。")
                return
            history = list(displayed)

            if target_qq:
                # 按指定用户筛选（基于快照，不重新加载）
                target_messages = [msg for msg in history if msg[3] == str(target_qq)]

                success = 0
                failed_msgs = []
                invalid_nums = []
                for num in numbers:
                    idx = num - 1
                    if 0 <= idx < len(target_messages):
                        msg_id = target_messages[idx][0]
                        ok, err = await self._do_recall(event.bot, msg_id)
                        if ok:
                            success += 1
                            self._remove_message_from_history(session_id, msg_id)
                        else:
                            failed_msgs.append(f"[{num}]{err}")
                            if err in {
                                self._RECALL_TIMEOUT_ERROR,
                                self._RECALL_UNCONFIRMED_ERROR,
                            }:
                                break
                    else:
                        invalid_nums.append(str(num))

                if self.conf.get("enable_recall_notification", True):
                    result_text = f"✅ 已成功撤回指定用户的 {success} 条消息。"
                    if invalid_nums:
                        result_text += f"\n❌ 无效编号: {', '.join(invalid_nums)}"
                    if failed_msgs:
                        result_text += f"\n⚠️ 失败: {', '.join(failed_msgs[:5])}"
                    self._mark_no_auto_recall(event)
                    yield event.plain_result(result_text)
                event.stop_event()
                return

            # 按编号撤回消息（所有消息），基于快照
            if not history:
                if self.conf.get("enable_recall_notification", True):
                    self._mark_no_auto_recall(event)
                    yield event.plain_result(
                        "没有可撤回的消息记录，请先发送「消息列表」查看。"
                    )
                event.stop_event()
                return

            success = 0
            failed_msgs = []
            invalid_nums = []
            recalled_ids = []

            for num in numbers:
                idx = num - 1
                if 0 <= idx < len(history):
                    msg_id = history[idx][0]
                    if msg_id in recalled_ids:
                        continue
                    recalled_ids.append(msg_id)
                    ok, err = await self._do_recall(event.bot, msg_id)
                    if ok:
                        success += 1
                        self._remove_message_from_history(session_id, msg_id)
                    else:
                        failed_msgs.append(f"[{num}]{err}")
                        if err in {
                            self._RECALL_TIMEOUT_ERROR,
                            self._RECALL_UNCONFIRMED_ERROR,
                        }:
                            break
                else:
                    invalid_nums.append(str(num))

            if self.conf.get("enable_recall_notification", True):
                valid_nums_str = ", ".join(
                    str(n) for n in numbers if str(n) not in invalid_nums
                )
                result_text = (
                    f"✅ 已成功撤回 {success} 条消息（编号: {valid_nums_str}）。"
                )
                if invalid_nums:
                    result_text += f"\n❌ 无效编号: {', '.join(invalid_nums)}"
                if failed_msgs:
                    result_text += f"\n⚠️ 失败: {', '.join(failed_msgs[:5])}\n提示：机器人需要管理员权限才能撤回他人消息，且消息需在2分钟内"
                self._mark_no_auto_recall(event)
                yield event.plain_result(result_text)
            event.stop_event()

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("撤回")
    @_finish_command
    async def recall_reply_command(self, event: AstrMessageEvent):
        """
        撤回引用的消息:引用一条消息并发送「撤回」
        """
        logger.debug("[撤回] 命令被触发")

        if not isinstance(event, AiocqhttpMessageEvent):
            self._mark_no_auto_recall(event)
            yield event.plain_result("当前平台不支持此功能，仅支持 QQ 协议端。")
            return

        messages = event.get_messages()
        reply_id = None

        logger.debug(f"[撤回] 消息段数量: {len(messages)}")
        for i, segment in enumerate(messages):
            logger.debug(f"[撤回] 段[{i}]: type={type(segment).__name__}")

        for segment in messages:
            if isinstance(segment, Reply):
                reply_id = int(segment.id)
                logger.debug(f"[撤回] 找到Reply段, reply_id={reply_id}")
                break

        if not reply_id:
            logger.debug("[撤回] 未找到Reply段，提示使用方法")
            self._mark_no_auto_recall(event)
            yield event.plain_result(
                "使用方法：引用一条消息，然后发送「撤回」即可撤回该消息。\n"
                "提示：需要管理员权限"
            )
            return

        async for result in self._recall_reply(event, reply_id):
            yield result
        event.stop_event()
