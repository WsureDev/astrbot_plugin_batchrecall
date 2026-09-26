"""无需安装 AstrBot；可通过 ASTRBOT_SOURCE 指向源码，运行真实调度方法回归。"""

import ast
import asyncio
import importlib.util
import inspect
import logging
import os
import subprocess
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, patch

ROOT = Path(__file__).resolve().parents[1]


class Plain:
    def __init__(self, text):
        self.text = text


class Reply:
    def __init__(self, id):
        self.id = id


class At:
    def __init__(self, qq):
        self.qq = qq


class MessageChain:
    def __init__(self, chain=None):
        self.chain = chain or []
        self.stopped = False

    def message(self, text):
        self.chain.append(Plain(text))
        return self

    def stop_event(self):
        self.stopped = True
        return self

    def is_stopped(self):
        return self.stopped


class Event:
    def __init__(self, text="/other", group="123", messages=None):
        self.text = text
        self.group = group
        self.messages = messages if messages is not None else [Plain(text)]
        self._extras = {}
        self._result = None
        self._force_stopped = False
        self._has_send_oper = False
        self.call_llm = False
        self.is_at_or_wake_command = True
        self.admin = True
        self.bot = SimpleNamespace(
            call_action=AsyncMock(return_value={"message_id": 901}),
            delete_msg=AsyncMock(),
        )
        self.message_obj = SimpleNamespace(
            raw_message={
                "message_id": 900,
                "message": [{"type": "text", "data": {"text": text}}],
                "sender": {"user_id": 42, "nickname": "测试用户", "role": "member"},
            }
        )

    def get_extra(self, key, default=None):
        return self._extras.get(key, default)

    def set_extra(self, key, value):
        self._extras[key] = value

    def get_group_id(self):
        return self.group

    def get_sender_id(self):
        return "42"

    def get_self_id(self):
        return "100"

    def get_messages(self):
        return self.messages

    def get_message_str(self):
        return self.text

    def is_admin(self):
        return self.admin

    def plain_result(self, text):
        return MessageChain().message(text)

    def set_result(self, result):
        self._result = result

    def get_result(self):
        return self._result

    def clear_result(self):
        self._result = None

    def stop_event(self):
        if self._result is None:
            self._result = MessageChain()
        self._result.stop_event()

    def is_stopped(self):
        return bool(self._result and self._result.is_stopped())

    def should_call_llm(self, value):
        self.call_llm = value


class QQEvent(Event):
    @staticmethod
    async def _parse_onebot_json(chain):
        return [{"type": "text", "data": {"text": c.text}} for c in chain.chain]


class CommandFilter:
    pass


class CommandGroupFilter:
    pass


class Star:
    def __init__(self, context):
        pass


def decorator(*args, **kwargs):
    return lambda handler: handler


def load_plugin():
    modules = {}
    exports = {
        "aiocqhttp.exceptions": {
            "ActionFailed": type("ActionFailed", (Exception,), {})
        },
        "astrbot.api": {"logger": logging.getLogger("batchrecall-test")},
        "astrbot.api.event": {
            "AstrMessageEvent": Event,
            "filter": SimpleNamespace(
                command=decorator,
                permission_type=decorator,
                platform_adapter_type=decorator,
                on_decorating_result=decorator,
                PlatformAdapterType=SimpleNamespace(ALL=0, AIOCQHTTP=1),
                PermissionType=SimpleNamespace(ADMIN=1),
            ),
        },
        "astrbot.api.star": {"Star": Star, "Context": object, "register": decorator},
        "astrbot.core.message.components": {"Plain": Plain, "At": At, "Reply": Reply},
        "astrbot.core.message.message_event_result": {"MessageChain": MessageChain},
        "astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event": {
            "AiocqhttpMessageEvent": QQEvent,
        },
        "astrbot.core.star.filter.command": {"CommandFilter": CommandFilter},
        "astrbot.core.star.filter.command_group": {
            "CommandGroupFilter": CommandGroupFilter
        },
    }
    for name, attrs in exports.items():
        parts = name.split(".")
        for i in range(1, len(parts) + 1):
            key = ".".join(parts[:i])
            modules.setdefault(key, ModuleType(key))
        modules[name].__dict__.update(attrs)
    source = Path(os.environ.get("BATCHRECALL_SOURCE", ROOT / "main.py"))
    spec = importlib.util.spec_from_file_location("batchrecall_under_test", source)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


PLUGIN = load_plugin()


def plugin(notification=True):
    return PLUGIN.BatchRecall(
        None,
        {
            "recall_time": 60,
            "enable_group_recall": False,
            "enable_private_recall": False,
            "enable_recall_notification": notification,
        },
    )


async def collect(handler, event):
    return [result async for result in handler(event)]


async def stalled_api(*args, **kwargs):
    await asyncio.Event().wait()


def metadata(handler, command=False):
    return SimpleNamespace(
        handler=handler,
        handler_full_name=handler.__name__,
        handler_module_path="test",
        handler_name=handler.__name__,
        event_filters=[CommandFilter()] if command else [],
    )


class PluginTests(unittest.IsolatedAsyncioTestCase):
    async def test_registered_commands_disable_only_default_llm(self):
        for group in ("123", ""):
            event = QQEvent(group=group)
            event.set_extra(
                "activated_handlers", [metadata(lambda: None, command=True)]
            )
            await collect(plugin().on_message, event)
            self.assertTrue(event.call_llm)
            self.assertFalse(event.is_stopped())

    async def test_normal_chat_and_unmatched_prefix_remain_eligible_for_llm(self):
        for text in ("你好", "/未知指令", "撤回这段话是什么意思"):
            event = QQEvent(text)
            await collect(plugin().on_message, event)
            self.assertFalse(event.call_llm)
            self.assertFalse(event.is_stopped())

    async def test_direct_send_marks_even_response_without_message_id(self):
        for response in ({"message_id": 901}, {}):
            event = QQEvent()
            event.bot.call_action.return_value = response
            event.set_result(event.plain_result("其他插件回复"))
            await plugin().intercept_and_recall(event)
            self.assertTrue(event._has_send_oper)
            self.assertEqual(event.get_result().chain, [])
            event.bot.call_action.assert_awaited_once()

    async def test_failed_send_preserves_chain_for_framework(self):
        event = QQEvent()
        event.bot.call_action.side_effect = RuntimeError("发送失败")
        event.set_result(event.plain_result("仍需发送"))
        await plugin().intercept_and_recall(event)
        self.assertFalse(event._has_send_oper)
        self.assertEqual(event.get_result().chain[0].text, "仍需发送")

    async def test_silent_recall_variants(self):
        cases = [
            ("recall_reply_command", "撤回", [Reply(11), Plain("撤回")]),
            ("on_message", "撤回", [Reply(11), Plain("撤回")]),
            ("batch_recall_command", "批量撤回", [Reply(11), Plain("批量撤回")]),
            ("batch_recall_command", "批量撤回 1", None),
            ("batch_recall_command", "批量撤回 1,2", None),
            ("batch_recall_command", "批量撤回 1,2", [Plain("批量撤回 1,2"), At(42)]),
            ("recall_bot_messages_command", "撤回自身 1", None),
        ]
        for name, text, messages in cases:
            with self.subTest(handler=name, text=text, messages=messages):
                p = plugin(notification=False)
                p.message_history["123"] = [
                    (11, "消息1", 0, "42", "用户", True),
                    (12, "消息2", 0, "42", "用户", False),
                ]
                event = QQEvent(text, messages=messages)
                self.assertEqual(await collect(getattr(p, name), event), [])
                self.assertGreater(event.bot.delete_msg.await_count, 0)
                self.assertTrue(event.call_llm)
                self.assertTrue(event.is_stopped())
                event.bot.call_action.assert_not_awaited()

    async def test_empty_history_and_help_do_not_fall_through(self):
        for notification in (True, False):
            for name, text in (
                ("batch_recall_command", "批量撤回 1"),
                ("recall_bot_messages_command", "撤回自身 1"),
                ("recall_reply_command", "撤回"),
                ("message_list_command", "消息列表"),
            ):
                event = QQEvent(text)
                event.bot.call_action.return_value = {"messages": []}
                await collect(getattr(plugin(notification), name), event)
                event.clear_result()  # 旧框架会清除停止状态，禁用 LLM 的标记必须仍在。
                self.assertTrue(event.call_llm, (notification, name))

    async def test_reply_fallback_denial_stops_without_deleting(self):
        event = QQEvent("撤回", messages=[Reply(11), Plain("撤回")])
        event.admin = False
        self.assertEqual(len(await collect(plugin().on_message, event)), 1)
        self.assertTrue(event.call_llm)
        self.assertTrue(event.is_stopped())
        event.bot.delete_msg.assert_not_awaited()

    async def test_notifications_and_automatic_recall_still_work(self):
        p = plugin()
        event = QQEvent("撤回", messages=[Reply(11), Plain("撤回")])
        results = await collect(p.recall_reply_command, event)
        self.assertIn("已撤回", results[0].chain[0].text)
        p = plugin()
        p.conf["enable_group_recall"] = True
        p.conf["recall_time"] = 0
        event = QQEvent()
        event.set_result(event.plain_result("普通回复"))
        await p.intercept_and_recall(event)
        await asyncio.gather(*p.recall_tasks)
        event.bot.delete_msg.assert_awaited_once_with(message_id=901)

    async def test_reply_is_handled_once_even_if_stop_state_is_cleared(self):
        for notification in (True, False):
            for first, second in (
                ("on_message", "recall_reply_command"),
                ("recall_reply_command", "on_message"),
            ):
                with self.subTest(notification=notification, first=first):
                    p = plugin(notification)
                    event = QQEvent("撤回", messages=[Reply(11), Plain("撤回")])
                    results = await collect(getattr(p, first), event)
                    event.clear_result()  # 旧框架在两个处理器之间清除停止状态。
                    results += await collect(getattr(p, second), event)
                    event.bot.delete_msg.assert_awaited_once_with(message_id=11)
                    self.assertEqual(len(results), int(notification))

    async def test_reply_fallback_matches_complete_plain_text(self):
        for parts, expected in (
            ([" /撤", "回 "], 1),
            (["撤回", "是什么意思"], 0),
            (["撤回", "自身 1"], 0),
        ):
            with self.subTest(parts=parts):
                event = QQEvent(messages=[Reply(11), At(100)] + [Plain(t) for t in parts])
                await collect(plugin().on_message, event)
                self.assertEqual(event.bot.delete_msg.await_count, expected)

    async def test_failed_reply_keeps_message_for_retry(self):
        for handler in ("on_message", "recall_reply_command", "batch_recall_command"):
            with self.subTest(handler=handler):
                p = plugin()
                history = [(11, "消息", 0, "100", "机器人", True)]
                p.message_history["123"] = history.copy()
                event = QQEvent("撤回", messages=[Reply(11), Plain("撤回")])
                event.bot.delete_msg.side_effect = RuntimeError("协议端错误")
                await collect(getattr(p, handler), event)
                self.assertEqual(p.message_history["123"], history)

    async def test_stalled_reply_finishes_and_next_self_recall_still_works(self):
        for notification in (True, False):
            for handler in ("on_message", "recall_reply_command", "batch_recall_command"):
                with self.subTest(notification=notification, handler=handler):
                    p = plugin(notification)
                    p.conf["api_timeout"] = 0.01
                    history = [(11, "消息", 0, "100", "机器人", True)]
                    p.message_history["123"] = history.copy()
                    event = QQEvent("撤回", messages=[Reply(11), Plain("撤回")])
                    cancelled = asyncio.Event()

                    async def stalled(**kwargs):
                        try:
                            await asyncio.Event().wait()
                        finally:
                            cancelled.set()

                    event.bot.delete_msg.side_effect = stalled
                    results = await asyncio.wait_for(
                        collect(getattr(p, handler), event), timeout=0.3
                    )
                    self.assertTrue(cancelled.is_set())
                    self.assertTrue(event.is_stopped())
                    self.assertTrue(event.call_llm)
                    self.assertEqual(len(results), int(notification))
                    if notification:
                        self.assertIn("超时", results[0].chain[0].text)
                    self.assertEqual(p.message_history["123"], history)

                    next_event = QQEvent("撤回自身 1")
                    next_event.bot = event.bot
                    next_event.bot.delete_msg.side_effect = None
                    await asyncio.wait_for(
                        collect(p.recall_bot_messages_command, next_event), timeout=0.3
                    )
                    self.assertEqual(next_event.bot.delete_msg.await_count, 2)
                    self.assertEqual(p.message_history["123"], [])
                    self.assertTrue(next_event.is_stopped())

    async def test_stalled_batch_stops_at_first_timeout(self):
        for handler, text, messages in (
            ("recall_bot_messages_command", "撤回自身 2", None),
            ("batch_recall_command", "批量撤回 2", None),
            ("batch_recall_command", "批量撤回 1,2", None),
            ("batch_recall_command", "批量撤回 1,2", [Plain("批量撤回 1,2"), At(100)]),
        ):
            with self.subTest(handler=handler, text=text, messages=messages):
                p = plugin()
                p.conf["api_timeout"] = 0.01
                history = [
                    (11, "消息1", 0, "100", "机器人", True),
                    (12, "消息2", 0, "100", "机器人", True),
                ]
                p.message_history["123"] = history.copy()
                event = QQEvent(text, messages=messages)
                event.bot.delete_msg.side_effect = stalled_api
                results = await asyncio.wait_for(
                    collect(getattr(p, handler), event), timeout=0.3
                )
                event.bot.delete_msg.assert_awaited_once_with(message_id=11)
                self.assertEqual(p.message_history["123"], history)
                self.assertIn("超时", results[0].chain[0].text)
                self.assertTrue(event.is_stopped())

    async def test_stalled_send_is_not_retried_by_framework(self):
        for group in ("123", ""):
            with self.subTest(group=group):
                p = plugin()
                p.conf["api_timeout"] = 0.01
                event = QQEvent(group=group)
                event.set_result(event.plain_result("撤回结果"))
                event.bot.call_action.side_effect = stalled_api
                await asyncio.wait_for(p.intercept_and_recall(event), timeout=0.3)
                self.assertTrue(event._has_send_oper)
                self.assertEqual(event.get_result().chain, [])
                self.assertEqual(p.message_history, {})
                self.assertEqual(p.recall_tasks, set())
                event.bot.call_action.assert_awaited_once()

                next_event = QQEvent(group=group)
                next_event.bot = event.bot
                next_event.bot.call_action.side_effect = None
                next_event.set_result(next_event.plain_result("后续回复"))
                await p.intercept_and_recall(next_event)
                self.assertTrue(next_event._has_send_oper)
                session = group or "42"
                self.assertEqual(p.message_history[session][0][0], 901)

    async def test_stalled_history_queries_finish_without_deleting(self):
        for handler, text in (
            ("message_list_command", "消息列表"),
            ("recall_bot_messages_command", "撤回自身 1"),
            ("batch_recall_command", "批量撤回 1"),
        ):
            for cached in (False, True):
                with self.subTest(handler=handler, cached=cached):
                    p = plugin()
                    p.conf["api_timeout"] = 0.01
                    if cached:
                        p.message_history["123"] = [(12, "用户消息", 0, "42", "用户", False)]
                    event = QQEvent(text)
                    event.bot.call_action.side_effect = stalled_api
                    await asyncio.wait_for(collect(getattr(p, handler), event), timeout=0.3)
                    self.assertTrue(event.is_stopped())
                    self.assertTrue(event.call_llm)
                    if handler != "batch_recall_command" or not cached:
                        event.bot.delete_msg.assert_not_awaited()

    async def test_stalled_automatic_recall_finishes_and_unload_cancels_tasks(self):
        for unload in (False, True):
            with self.subTest(unload=unload):
                p = plugin()
                p.conf.update(enable_group_recall=True, recall_time=0, api_timeout=0.01)
                event = QQEvent()
                event.bot.delete_msg.side_effect = stalled_api
                event.set_result(event.plain_result("自动撤回的消息"))
                await p.intercept_and_recall(event)
                tasks = list(p.recall_tasks)
                self.assertEqual(len(tasks), 1)
                if unload:
                    await asyncio.wait_for(p.terminate(), timeout=0.3)
                    self.assertTrue(tasks[0].cancelled())
                else:
                    await asyncio.wait_for(asyncio.gather(*tasks), timeout=0.3)
                self.assertEqual(p.recall_tasks, set())
                self.assertEqual(p.message_history["123"][0][0], 901)


def load_framework(ref):
    """执行上游原始方法体，只替换外部依赖，避免安装完整服务及 LLM SDK。"""
    source_root = os.environ["ASTRBOT_SOURCE"]
    namespace = {
        "inspect": inspect,
        "logger": logging.getLogger("pipeline-test"),
        "MessageEventResult": MessageChain,
        "CommandResult": type("CommandResult", (), {}),
        "ProviderRequest": type("ProviderRequest", (), {}),
        "Stage": object,
        "star_map": {"test": SimpleNamespace(name="test")},
    }
    selections = {
        "astrbot/core/platform/astr_message_event.py": {
            "AstrMessageEvent": {
                "set_result",
                "get_result",
                "clear_result",
                "stop_event",
                "is_stopped",
                "should_call_llm",
            }
        },
        "astrbot/core/pipeline/context_utils.py": {"call_handler": None},
        "astrbot/core/pipeline/process_stage/method/star_request.py": {
            "StarRequestSubStage": {"process"}
        },
        "astrbot/core/pipeline/process_stage/stage.py": {"ProcessStage": {"process"}},
    }
    for path, names in selections.items():
        source = subprocess.check_output(
            ["git", "-C", source_root, "show", f"{ref}:{path}"], text=True
        )
        tree = ast.parse(source, filename=path)
        nodes = []
        for node in tree.body:
            if getattr(node, "name", None) not in names:
                continue
            node.decorator_list = []
            if isinstance(node, ast.ClassDef):
                node.bases = []
                node.body = [
                    n for n in node.body if getattr(n, "name", None) in names[node.name]
                ]
            nodes.append(node)
        tree.body = ast.parse("from __future__ import annotations").body + nodes
        exec(compile(ast.fix_missing_locations(tree), path, "exec"), namespace)  # noqa: S102 -- 执行指定上游源码的方法体
    real_event = namespace["AstrMessageEvent"]
    namespace["Event"] = type("SourceEvent", (real_event, QQEvent), {})
    return namespace


@unittest.skipUnless(
    os.environ.get("ASTRBOT_SOURCE"), "设置 ASTRBOT_SOURCE 以运行上游源码回归"
)
class FrameworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_source_pipeline(self):
        refs = os.environ.get("ASTRBOT_REFS", "v4.5.0,v4.20.0,HEAD").split(",")
        for ref in refs:
            for scenario in (
                "external_reply",
                "external_regex_reply",
                "external_silent",
                "external_explicit_llm",
                "chat",
                "silent_recall",
                "recall_help",
                "reply_recall",
                "reply_notification",
                "reply_timeout",
            ):
                for listener_first in (True, False):
                    with self.subTest(
                        ref=ref, scenario=scenario, listener_first=listener_first
                    ):
                        ns = load_framework(ref)
                        event = ns["Event"]()
                        p = plugin(notification=False)
                        llm_calls = []
                        command_calls = []

                        async def other_command(
                            event,
                            scenario=scenario,
                            command_calls=command_calls,
                            request_type=ns["ProviderRequest"],
                        ):
                            command_calls.append(True)
                            if scenario in ("external_reply", "external_regex_reply"):
                                yield event.plain_result("其他插件回复")
                            elif scenario == "external_silent":
                                yield None
                            elif scenario == "external_explicit_llm":
                                yield request_type()
                            event.stop_event()

                        async def llm_process(event, llm_calls=llm_calls):
                            llm_calls.append(True)
                            yield

                        handlers = [metadata(p.on_message)]
                        if scenario.startswith("external"):
                            handlers.append(
                                metadata(
                                    other_command, scenario != "external_regex_reply"
                                )
                            )
                        elif scenario == "silent_recall":
                            event.messages = [Reply(11), Plain("批量撤回")]
                            handlers.append(metadata(p.batch_recall_command, True))
                        elif scenario == "recall_help":
                            handlers.append(metadata(p.recall_reply_command, True))
                        elif scenario.startswith("reply_"):
                            event.messages = [Reply(11), Plain("撤回")]
                            p.conf["enable_recall_notification"] = scenario == "reply_notification"
                            if scenario == "reply_timeout":
                                p.conf["api_timeout"] = 0.01
                                event.bot.delete_msg.side_effect = stalled_api
                            handlers.append(metadata(p.recall_reply_command, True))
                        if not listener_first:
                            handlers.reverse()
                        event.set_extra("activated_handlers", handlers)
                        process = ns["ProcessStage"]()
                        process.ctx = SimpleNamespace(
                            astrbot_config={"provider_settings": {"enable": True}},
                            plugin_manager=SimpleNamespace(
                                context=SimpleNamespace(get_using_provider=lambda: True)
                            ),
                        )
                        process.star_request_sub_stage = ns["StarRequestSubStage"]()
                        process.agent_sub_stage = process.llm_request_sub_stage = (
                            SimpleNamespace(process=llm_process)
                        )
                        # 和调度器一样，yield 时执行发送阶段；STOP 时不再进入后续阶段。
                        generator = process.process(event)
                        try:
                            async for _ in generator:
                                if event.is_stopped():
                                    break
                                await p.intercept_and_recall(event)
                                result = event.get_result()
                                if result and result.chain:
                                    self.fail("成功拦截后不应再由框架重复发送")
                                event.clear_result()
                        finally:
                            await generator.aclose()
                        expected_llm = scenario in ("chat", "external_explicit_llm")
                        self.assertEqual(len(llm_calls), int(expected_llm))
                        if scenario.startswith("external"):
                            self.assertEqual(command_calls, [True])
                        if scenario in (
                            "external_reply",
                            "external_regex_reply",
                            "recall_help",
                            "reply_notification",
                        ):
                            event.bot.call_action.assert_awaited_once()
                        if scenario == "silent_recall":
                            event.bot.delete_msg.assert_awaited_once_with(message_id=11)
                            event.bot.call_action.assert_not_awaited()
                        if scenario.startswith("reply_"):
                            event.bot.delete_msg.assert_awaited_once_with(message_id=11)
                            if scenario != "reply_notification":
                                event.bot.call_action.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
