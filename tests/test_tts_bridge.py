"""Run the real TTS send wrappers and conversion against OneBot receipts."""

import asyncio
import os
import secrets
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from support import (
    LOGGER,
    At,
    Bot,
    Context,
    MessageChain,
    Plain,
    QQEvent,
    RawEvent,
    Record,
    RespondStage,
    load_package,
    methods,
    plugin,
)

TTS_SOURCE = os.environ.get("TTS_SOURCE")
FRAMEWORK = os.environ.get("ASTRBOT_SOURCE")


@contextmanager
def source_transport():
    if not FRAMEWORK:
        yield
        return
    root = Path(FRAMEWORK)
    if (root / "astrbot").is_dir():
        root = root / "astrbot"
    path = root / "core/platform/sources/aiocqhttp/aiocqhttp_message_event.py"
    names = {
        "_from_segment_to_dict",
        "_parse_onebot_json",
        "_dispatch_send",
        "send_message",
    }
    namespace = {
        "At": At,
        "Plain": Plain,
        "Record": Record,
        "Event": RawEvent,
        "asyncio": asyncio,
    }
    for name in ("Image", "File", "Node", "Nodes", "Video"):
        namespace[name] = type(name, (), {})
    actual = methods(path, "AiocqhttpMessageEvent", names, namespace)
    originals = {name: QQEvent.__dict__.get(name) for name in names}
    for name in names:
        setattr(QQEvent, name, actual.__dict__[name])
    try:
        yield
    finally:
        for name, original in originals.items():
            if original is None:
                delattr(QQEvent, name)
            else:
                setattr(QQEvent, name, original)


@unittest.skipUnless(TTS_SOURCE, "Set TTS_SOURCE to the short TTS repository")
class TtsBridgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_delivery_in_both_plugin_load_orders(self):
        root = Path(TTS_SOURCE)
        outgoing = load_package("tts_bridge_under_test", root, "outgoing")
        origins = sys.modules["tts_bridge_under_test.origins"]

        class GsviError(Exception):
            pass

        namespace = {
            "asyncio": asyncio,
            "secrets": secrets,
            "Plain": Plain,
            "Record": Record,
            "CURRENT_TOOL": origins.CURRENT_TOOL,
            "logger": LOGGER,
            "GsviError": GsviError,
        }
        model = methods(
            root / "main.py", "GsviShortTtsPlugin", {"convert_chain"}, namespace
        )
        scenarios = (
            "converted",
            "disabled",
            "over_limit",
            "mixed",
            "http_failure",
            "streaming",
        )
        for recall_first in (True, False):
            for scenario in scenarios:
                with (
                    self.subTest(recall_first=recall_first, scenario=scenario),
                    source_transport(),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    if recall_first:
                        recall, bot, context = await plugin(automatic=True)
                    else:
                        bot = Bot()
                        context = Context(bot)
                    syntheses = []
                    paths = []

                    async def synthesize(text, voice, output):
                        syntheses.append(text)
                        if scenario == "http_failure":
                            raise GsviError("offline HTTP failure")
                        path = Path(directory) / "voice.wav"
                        path.write_bytes(b"RIFF" + b"\x00" * 4 + b"WAVE" + b"\x00" * 32)
                        paths.append(path)
                        return path

                    converter = model()
                    converter.settings = SimpleNamespace(timeout_ms=1000)
                    converter.client = SimpleNamespace(synthesize=synthesize)
                    converter.directory = Path(directory)
                    policy = SimpleNamespace(
                        voice="花火", max_chars=120, probability=100
                    )
                    if scenario == "disabled":
                        policy.probability = 0

                    async def convert(channel, chain):
                        return await converter.convert_chain(chain, policy)

                    async def enabled(channel):
                        return policy.probability > 0

                    tts = outgoing.OutgoingTts(context, convert, enabled)
                    tts.install()
                    if not recall_first:
                        recall = sys.modules["batchrecall_under_test.main"].BatchRecall(
                            context,
                            {
                                "recall_time": 60,
                                "enable_group_recall": True,
                                "confirmation_timeout": 0.01,
                            },
                        )
                        await recall.initialize()
                    try:
                        event = QQEvent(bot=bot)
                        text = "这是模型最终回复"
                        if scenario == "over_limit":
                            text = "长" * 121
                        chain = MessageChain().message(text)
                        if scenario == "mixed":
                            chain.chain.append(At(42))
                        event.set_result(chain)
                        origins.mark_request_origin(event)
                        if scenario == "streaming":
                            chain.result_content_type = (
                                origins.ResultContentType.STREAMING_RESULT
                            )

                            async def frames():
                                yield MessageChain().message("这是模型")
                                yield MessageChain().message("最终回复")

                            chain.async_stream = frames()
                            await event.send_streaming(chain.async_stream, True)
                        else:
                            await RespondStage().process(event)
                        sends = [
                            (action, data)
                            for action, data in bot.api.calls
                            if action.startswith("send_")
                        ]
                        self.assertEqual(len(sends), 1)
                        message = sends[0][1]["message"]
                        expected = "text"
                        if scenario in {"converted", "streaming"}:
                            expected = "record"
                        self.assertEqual(message[0]["type"], expected)
                        self.assertEqual(event.get_result().chain[0].text, text)
                        self.assertEqual(len(recall.recall_tasks), 1)
                        self.assertEqual(
                            recall.message_history["test:group:123"][0][0], 901
                        )
                        self.assertTrue(all(not path.exists() for path in paths))
                        if scenario in {"disabled", "over_limit", "mixed"}:
                            self.assertEqual(syntheses, [])
                        else:
                            self.assertEqual(len(syntheses), 1)
                    finally:
                        if recall_first:
                            tts.uninstall()
                            await recall.terminate()
                        else:
                            await recall.terminate()
                            tts.uninstall()


if __name__ == "__main__":
    unittest.main()
