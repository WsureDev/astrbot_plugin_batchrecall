"""Observe OneBot delivery receipts without sending or rewriting messages."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps

from astrbot.api.event import AstrMessageEvent
from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
    AiocqhttpMessageEvent,
)

NO_AUTO_RECALL = "batchrecall.no_auto_recall"
LISTING_KEY = "batchrecall.pending_listing"
_MISSING = object()


@dataclass(frozen=True)
class Destination:
    platform_id: str
    target_id: str
    is_group: bool

    @property
    def key(self) -> str:
        kind = "private"
        if self.is_group:
            kind = "group"
        return f"{self.platform_id}:{kind}:{self.target_id}"

    @property
    def umo(self) -> str:
        kind = "FriendMessage"
        if self.is_group:
            kind = "GroupMessage"
        return f"{self.platform_id}:{kind}:{self.target_id}"


def event_destination(event: AstrMessageEvent) -> Destination:
    group = str(event.get_group_id() or "")
    target = group or str(event.get_sender_id())
    return Destination(str(event.get_platform_id()), target, bool(group))


@dataclass(frozen=True)
class DeliveryScope:
    event: AstrMessageEvent
    task: asyncio.Task | None
    destination: Destination


CURRENT_DELIVERY: ContextVar[DeliveryScope | None] = ContextVar(
    "batchrecall_delivery", default=None
)


@dataclass(frozen=True)
class Receipt:
    destination: Destination
    message_id: int
    message: str | list[dict]
    event: AstrMessageEvent | None


class DeliveryObserver:
    """Wrap client instances and the event send scope, preserving API outcomes."""

    def __init__(
        self,
        bot,
        platform_id: str,
        received: Callable[[Receipt], Awaitable[None]],
        recalled: Callable[[dict], Awaitable[None]],
        allowed: Callable[[Destination], bool],
        logger,
    ) -> None:
        self.bot = bot
        self.platform_id = platform_id
        self.received = received
        self.recalled = recalled
        self.allowed = allowed
        self.logger = logger
        self.api = bot.api
        self.previous = vars(self.api).get("call_action", _MISSING)
        self.original = self.api.call_action
        self.wrapper = None
        self.active = False

    def install(self) -> None:
        if self.active:
            return
        self.active = True

        @wraps(self.original)
        async def call_action(action: str, **params):
            # The framework owns sending, cancellation and errors. Never retry.
            result = await self.original(action, **params)
            if self.active:
                try:
                    await self.observe(action, params, result)
                except Exception as error:
                    self.logger.warning(
                        f"[BatchRecall] reason=receipt_observer_failed "
                        f"error={type(error).__name__}"
                    )
            return result

        self.wrapper = call_action
        self.api.call_action = call_action
        for kind in ("notice.group_recall", "notice.friend_recall"):
            self.bot.subscribe(kind, self.on_notice)

    async def on_notice(self, notice: dict) -> None:
        if self.active:
            await self.recalled(notice)

    async def observe(self, action: str, params: dict, result) -> None:
        if action not in {
            "send_msg",
            "send_group_msg",
            "send_private_msg",
            "send_group_forward_msg",
            "send_private_forward_msg",
        }:
            return
        if not isinstance(result, dict) or not result.get("message_id"):
            return
        is_group = action in {"send_group_msg", "send_group_forward_msg"}
        if action == "send_msg":
            is_group = params.get("message_type") == "group" or bool(
                params.get("group_id")
            )
        target = params.get("user_id")
        if is_group:
            target = params.get("group_id")
        if target is None:
            return
        destination = Destination(self.platform_id, str(target), is_group)
        if not self.allowed(destination):
            return
        scope = CURRENT_DELIVERY.get()
        event = None
        if (
            scope is not None
            and scope.task is asyncio.current_task()
            and scope.destination == destination
        ):
            event = scope.event
            plugins = event.plugins_name
            if plugins is not None and plugins != ["*"]:
                if "astrbot_plugin_batchrecall" not in plugins:
                    return
        message = params.get("message", [])
        if "forward" in action:
            message = [{"type": "forward", "data": {}}]
        await self.received(
            Receipt(destination, int(result["message_id"]), message, event)
        )

    def uninstall(self) -> None:
        self.active = False
        for kind in ("notice.group_recall", "notice.friend_recall"):
            self.bot.unsubscribe(kind, self.on_notice)
        if vars(self.api).get("call_action") is self.wrapper:
            if self.previous is _MISSING:
                del self.api.call_action
            else:
                self.api.call_action = self.previous


class EventDeliveryScope:
    """Associate receipt metadata with the actual event.send call only."""

    def __init__(self) -> None:
        self.original = None
        self.wrapper = None
        self.active = False

    def install(self) -> None:
        if self.active:
            return
        self.active = True
        original = AiocqhttpMessageEvent.send
        self.original = original

        @wraps(original)
        async def send(event, message):
            if not self.active:
                return await original(event, message)
            token = CURRENT_DELIVERY.set(
                DeliveryScope(event, asyncio.current_task(), event_destination(event))
            )
            try:
                return await original(event, message)
            finally:
                CURRENT_DELIVERY.reset(token)

        self.wrapper = send
        AiocqhttpMessageEvent.send = send

    def uninstall(self) -> None:
        self.active = False
        if AiocqhttpMessageEvent.send is self.wrapper:
            AiocqhttpMessageEvent.send = self.original
