"""Offline AstrBot/OneBot fixtures; optional real deployed framework methods."""

from __future__ import annotations

import ast
import base64
import functools
import importlib.util
import logging
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger("batchrecall-test")


class ComponentType(str, Enum):
    Plain = "Plain"
    Record = "Record"
    At = "At"
    Reply = "Reply"
    File = "File"
    Image = "Image"
    Video = "Video"
    Node = "Node"
    Nodes = "Nodes"


class Plain:
    type = ComponentType.Plain

    def __init__(self, text):
        self.text = text

    def toDict(self):
        return {"type": "text", "data": {"text": self.text}}


class Record:
    type = ComponentType.Record

    def __init__(self, file, url=None, text=""):
        self.file, self.url, self.text = file, url, text

    async def convert_to_base64(self):
        return base64.b64encode(Path(self.file).read_bytes()).decode()


class Reply:
    type = ComponentType.Reply

    def __init__(self, id):
        self.id = id


class At:
    type = ComponentType.At

    def __init__(self, qq):
        self.qq = qq

    def toDict(self):
        return {"type": "at", "data": {"qq": self.qq}}


class MessageType(str, Enum):
    GROUP_MESSAGE = "GroupMessage"
    FRIEND_MESSAGE = "FriendMessage"


@dataclass
class MessageSession:
    platform_name: str
    message_type: MessageType
    session_id: str

    @property
    def platform_id(self):
        return self.platform_name

    def __str__(self):
        return f"{self.platform_name}:{self.message_type.value}:{self.session_id}"

    @classmethod
    def from_str(cls, value):
        platform, kind, target = value.split(":", 2)
        return cls(platform, MessageType(kind), target)


class ResultContentType(Enum):
    LLM_RESULT = 1
    STREAMING_RESULT = 2


class MessageChain:
    def __init__(self, chain=None, type=None):
        self.chain = list(chain or [])
        self.type = type
        self.use_t2i_ = None
        self.use_markdown_ = None
        self.result_content_type = ResultContentType.LLM_RESULT
        self.async_stream = None
        self.stopped = False

    def message(self, text):
        self.chain.append(Plain(text))
        return self

    def derive(self, chain=None):
        result = MessageChain(chain, self.type)
        result.use_t2i_, result.use_markdown_ = self.use_t2i_, self.use_markdown_
        return result

    def stop_event(self):
        self.stopped = True
        return self

    def is_stopped(self):
        return self.stopped

    def is_model_result(self):
        return self.result_content_type == ResultContentType.LLM_RESULT


class RawEvent(dict):
    pass


class Event:
    def __init__(self, text="你好", group="123", sender="42", bot=None, messages=None):
        self.text, self.group, self.sender = text, group, sender
        self.message_str = text
        self.messages = list(messages or [Plain(text)])
        self._extras, self._result = {}, None
        self._force_stopped = False
        self._has_send_oper = False
        self.call_llm = False
        self.is_at_or_wake_command = True
        self.plugins_name = None
        self.admin = True
        self.bot = bot or Bot()
        self.message_obj = SimpleNamespace(
            raw_message=RawEvent(
                post_type="message",
                message_id=900,
                message=[Plain(text).toDict()],
                sender={"user_id": sender, "nickname": "用户", "role": "member"},
                self_id="100",
            )
        )

    @property
    def unified_msg_origin(self):
        return str(self.session)

    @property
    def session(self):
        kind = MessageType.FRIEND_MESSAGE
        if self.group:
            kind = MessageType.GROUP_MESSAGE
        return MessageSession("test", kind, self.group or self.sender)

    def get_extra(self, key=None, default=None):
        if key is None:
            return self._extras
        return self._extras.get(key, default)

    def set_extra(self, key, value):
        self._extras[key] = value

    def get_platform_id(self):
        return "test"

    def get_platform_name(self):
        return "aiocqhttp"

    def get_group_id(self):
        return self.group

    def get_sender_id(self):
        return self.sender

    def get_self_id(self):
        return "100"

    def get_self_name(self):
        return "机器人"

    def get_messages(self):
        return self.messages

    def get_message_str(self):
        return self.text

    def is_private_chat(self):
        return not self.group

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
        self._force_stopped = True

    def is_stopped(self):
        return self._force_stopped

    def should_call_llm(self, value):
        self.call_llm = value

    async def send(self, message):
        self._has_send_oper = True


class QQEvent(Event):
    @classmethod
    async def send_message(
        cls, bot, message_chain, event=None, is_group=False, session_id=None
    ):
        message = []
        for item in message_chain.chain:
            if isinstance(item, Record):
                message.append(
                    {"type": "record", "data": {"file": await item.convert_to_base64()}}
                )
            else:
                message.append(item.toDict())
        if is_group:
            await bot.call_action(
                "send_group_msg", group_id=int(session_id), message=message
            )
        else:
            await bot.call_action(
                "send_private_msg", user_id=int(session_id), message=message
            )

    async def send(self, message):
        await self.send_message(
            self.bot,
            message,
            self.message_obj.raw_message,
            bool(self.group),
            self.group or self.sender,
        )
        await super().send(message)

    async def send_streaming(self, generator, use_fallback=False):
        async for chain in generator:
            if chain.chain and chain.type is None:
                await self.send(chain)


class ActionFailed(Exception):
    def __init__(self, result):
        self.result = result
        self.retcode = result["retcode"]


class Api:
    def __init__(self, bot):
        self.bot = bot
        self.calls = []
        self.counter = 900
        self.error = None
        self.confirm = True
        self.block = None
        self.group = 123
        self.history = []

    async def call_action(self, action, **params):
        self.calls.append((action, params))
        if self.block is not None:
            await self.block.wait()
        if self.error is not None:
            raise self.error
        if action == "delete_msg":
            if self.confirm:
                await self.bot.emit(
                    {
                        "notice_type": "group_recall",
                        "message_id": params["message_id"],
                        "group_id": self.group,
                        "user_id": 100,
                    }
                )
            return None
        if action == "get_group_msg_history":
            return {"messages": self.history}
        self.counter += 1
        return {"message_id": self.counter}


class Bot:
    def __init__(self):
        self.api = Api(self)
        self.subscribers = {}

    def __getattr__(self, action):
        return functools.partial(self.call_action, action)

    async def call_action(self, action, **params):
        return await self.api.call_action(action, **params)

    def subscribe(self, kind, callback):
        self.subscribers.setdefault(kind, set()).add(callback)

    def unsubscribe(self, kind, callback):
        self.subscribers[kind].discard(callback)

    async def emit(self, notice):
        for callback in tuple(
            self.subscribers.get("notice." + notice["notice_type"], ())
        ):
            await callback(notice)


class Platform:
    def __init__(self, bot):
        self.bot = bot

    def meta(self):
        return SimpleNamespace(name="aiocqhttp", id="test")

    async def send_by_session(self, session, chain):
        await QQEvent.send_message(
            self.bot,
            chain,
            is_group=session.message_type == MessageType.GROUP_MESSAGE,
            session_id=session.session_id,
        )


class Context:
    def __init__(self, bot):
        self.platform_manager = SimpleNamespace(platform_insts=[Platform(bot)])
        self.config = {"plugin_set": ["*"]}

    def get_platform_inst(self, platform_id):
        return self.platform_manager.platform_insts[0]

    def get_config(self, destination=None):
        return self.config

    async def send_message(self, session, chain):
        if isinstance(session, str):
            session = MessageSession.from_str(session)
        await self.get_platform_inst(session.platform_id).send_by_session(
            session, chain
        )
        return True


class RespondStage:
    async def process(self, event):
        await event.send(event.get_result().derive(event.get_result().chain))


class Star:
    def __init__(self, context):
        self.context = context


class CommandFilter:
    pass


class CommandGroupFilter:
    pass


def decorator(*args, **kwargs):
    return lambda handler: handler


def exports():
    return {
        "aiocqhttp": {"CQHttp": Bot, "Event": RawEvent},
        "aiocqhttp.exceptions": {"ActionFailed": ActionFailed},
        "astrbot.api": {"logger": LOGGER},
        "astrbot.api.event": {
            "AstrMessageEvent": Event,
            "filter": SimpleNamespace(
                command=decorator,
                permission_type=decorator,
                platform_adapter_type=decorator,
                on_platform_loaded=decorator,
                event_message_type=decorator,
                PlatformAdapterType=SimpleNamespace(AIOCQHTTP=1),
                PermissionType=SimpleNamespace(ADMIN=1),
            ),
        },
        "astrbot.api.star": {"Star": Star, "Context": Context, "register": decorator},
        "astrbot.api.message_components": {"Plain": Plain, "Record": Record},
        "astrbot.core.message.components": {"At": At, "Plain": Plain, "Reply": Reply},
        "astrbot.core.message.message_event_result": {
            "MessageChain": MessageChain,
            "ResultContentType": ResultContentType,
        },
        "astrbot.core.platform.astr_message_event": {"AstrMessageEvent": Event},
        "astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event": {
            "AiocqhttpMessageEvent": QQEvent
        },
        "astrbot.core.platform.message_session": {"MessageSession": MessageSession},
        "astrbot.core.platform.message_type": {"MessageType": MessageType},
        "astrbot.core.pipeline.respond.stage": {"RespondStage": RespondStage},
        "astrbot.core.star.filter.command": {"CommandFilter": CommandFilter},
        "astrbot.core.star.filter.command_group": {
            "CommandGroupFilter": CommandGroupFilter
        },
    }


def load_package(name, root, module="main"):
    modules = {}
    for full_name, values in exports().items():
        parts = full_name.split(".")
        for index in range(1, len(parts) + 1):
            key = ".".join(parts[:index])
            modules.setdefault(key, ModuleType(key))
        modules[full_name].__dict__.update(values)
    package = ModuleType(name)
    package.__path__ = [str(root)]
    sys.modules[name] = package
    full_name = name + "." + module
    spec = importlib.util.spec_from_file_location(full_name, root / (module + ".py"))
    result = importlib.util.module_from_spec(spec)
    sys.modules[full_name] = result
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(result)
        loaded = {
            key: value
            for key, value in sys.modules.items()
            if key.startswith(name + ".")
        }
    sys.modules.update(loaded)
    return result


PLUGIN = load_package("batchrecall_under_test", ROOT)


async def plugin(notification=False, automatic=False):
    bot = Bot()
    context = Context(bot)
    instance = PLUGIN.BatchRecall(
        context,
        {
            "recall_time": 60,
            "api_timeout": 0.05,
            "confirmation_timeout": 0.01,
            "enable_group_recall": automatic,
            "enable_private_recall": automatic,
            "enable_recall_notification": notification,
        },
    )
    await instance.initialize()
    return instance, bot, context


async def collect(handler, event):
    return [item async for item in handler(event)]


def methods(path, name, names, namespace):
    tree = ast.parse(path.read_text(), filename=str(path))
    node = next(node for node in tree.body if getattr(node, "name", "") == name)
    if isinstance(node, ast.ClassDef):
        node.bases = []
        node.decorator_list = []
        node.body = [part for part in node.body if getattr(part, "name", None) in names]
    tree.body = ast.parse("from __future__ import annotations").body + [node]
    exec(compile(ast.fix_missing_locations(tree), str(path), "exec"), namespace)
    return namespace[name]
