"""Exercise deployed AstrBot dispatch while a real per-session lock is held."""

import asyncio
import inspect
import os
import unittest
from pathlib import Path
from types import SimpleNamespace

from support import LOGGER, MessageChain, Plain, QQEvent, Reply, methods, plugin

SOURCE = os.environ.get("ASTRBOT_SOURCE")


@unittest.skipUnless(SOURCE, "Set ASTRBOT_SOURCE to an AstrBot source tree")
class SourcePipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_recall_requests_execute_while_llm_session_is_locked(self):
        root = Path(SOURCE)
        if (root / "astrbot").is_dir():
            root = root / "astrbot"
        namespace = {
            "inspect": inspect,
            "logger": LOGGER,
            "MessageEventResult": MessageChain,
            "CommandResult": type("CommandResult", (), {}),
            "ProviderRequest": type("ProviderRequest", (), {}),
            "star_map": {"test": SimpleNamespace(name="test")},
        }
        methods(
            root / "core/pipeline/context_utils.py", "call_handler", None, namespace
        )
        star = methods(
            root / "core/pipeline/process_stage/method/star_request.py",
            "StarRequestSubStage",
            {"process"},
            namespace,
        )
        process_type = methods(
            root / "core/pipeline/process_stage/stage.py",
            "ProcessStage",
            {"process"},
            namespace,
        )
        lock_namespace = {"__name__": "test_astrbot_session_lock"}
        lock_path = root / "core/utils/session_lock.py"
        exec(compile(lock_path.read_text(), str(lock_path), "exec"), lock_namespace)
        manager = lock_namespace["SessionLockManager"]()
        recall, bot, context = await plugin()
        queued = asyncio.Event()
        entered = asyncio.Event()

        async def llm(event):
            queued.set()
            async with manager.acquire_lock(event.unified_msg_origin):
                entered.set()
                yield

        async def dispatch(event):
            handler = SimpleNamespace(
                handler=recall.on_message,
                handler_full_name="on_message",
                handler_module_path="test",
                handler_name="on_message",
                event_filters=[],
            )
            event.set_extra("activated_handlers", [handler])
            stage = process_type()
            stage.ctx = SimpleNamespace(
                astrbot_config={"provider_settings": {"enable": True}}
            )
            stage.star_request_sub_stage = star()
            stage.agent_sub_stage = SimpleNamespace(process=llm)
            async for _ in stage.process(event):
                if event.is_stopped():
                    break

        try:
            async with manager.acquire_lock("test:GroupMessage:123"):
                for message_id in (11, 12):
                    event = QQEvent(
                        "撤回", bot=bot, messages=[Reply(message_id), Plain("撤回")]
                    )
                    await asyncio.wait_for(dispatch(event), 0.3)
                self.assertFalse(queued.is_set())
                chat = asyncio.create_task(dispatch(QQEvent(bot=bot)))
                await asyncio.wait_for(queued.wait(), 0.3)
                self.assertFalse(entered.is_set())
            await asyncio.wait_for(chat, 0.3)
            self.assertTrue(entered.is_set())
            self.assertEqual(
                [
                    data["message_id"]
                    for action, data in bot.api.calls
                    if action == "delete_msg"
                ],
                [11, 12],
            )
        finally:
            await recall.terminate()


if __name__ == "__main__":
    unittest.main()
