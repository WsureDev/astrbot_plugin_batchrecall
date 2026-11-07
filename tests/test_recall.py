import asyncio
import unittest

from support import ActionFailed, At, Plain, QQEvent, Reply, collect, plugin


class RecallTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.plugin, self.bot, self.context = await plugin()

    async def asyncTearDown(self):
        await self.plugin.terminate()

    def event(self, text="撤回", group="123", sender="42", reply=11):
        messages = [Plain(text)]
        if reply is not None:
            messages.insert(0, Reply(reply))
        return QQEvent(text, group, sender, self.bot, messages)

    def history(self, event):
        key = self.plugin._get_session_id(event)
        rows = [(11, "A", 1, "42", "用户", False), (12, "B", 1, "100", "机器人", True)]
        self.plugin.message_history[key] = rows.copy()
        return key, rows

    async def test_consecutive_reply_requests_are_independent_and_confirmed(self):
        for target in (11, 12):
            event = self.event(reply=target)
            result = await collect(self.plugin.on_message, event)
            self.assertEqual(result, [])
            self.assertTrue(event.call_llm)
            self.assertTrue(event.is_stopped())
        self.assertEqual(
            [
                params["message_id"]
                for action, params in self.bot.api.calls
                if action == "delete_msg"
            ],
            [11, 12],
        )

    async def test_two_handlers_for_one_event_delete_only_once(self):
        event = self.event()
        await collect(self.plugin.on_message, event)
        event.clear_result()
        await collect(self.plugin.recall_reply_command, event)
        self.assertEqual(len(self.bot.api.calls), 1)

    async def test_unconfirmed_recall_preserves_history_and_remains_silent(self):
        event = self.event()
        key, rows = self.history(event)
        self.bot.api.confirm = False
        with self.assertLogs("batchrecall-test", "WARNING") as logs:
            result = await collect(self.plugin.on_message, event)
        self.assertEqual(result, [])
        self.assertEqual(self.plugin.message_history[key], rows)
        self.assertIn("recall_unconfirmed", "\n".join(logs.output))
        self.assertEqual(self.plugin.confirmations.waiters, {})

    async def test_protocol_error_logs_reason_while_chat_stays_silent(self):
        event = self.event()
        key, rows = self.history(event)
        self.bot.api.error = ActionFailed(
            {"status": "failed", "retcode": 1200, "wording": "message expired"}
        )
        with self.assertLogs("batchrecall-test", "WARNING") as logs:
            self.assertEqual(await collect(self.plugin.on_message, event), [])
        self.assertEqual(self.plugin.message_history[key], rows)
        self.assertIn("message expired", "\n".join(logs.output))

    async def test_batch_error_does_not_delete_history_or_try_older_messages(self):
        event = self.event("批量撤回 1", reply=None)
        key, rows = self.history(event)
        self.bot.api.error = ActionFailed({"status": "failed", "retcode": 1200})
        with self.assertLogs("batchrecall-test", "WARNING"):
            await collect(self.plugin.batch_recall_command, event)
        self.assertEqual(len(self.bot.api.calls), 1)
        self.assertEqual(self.plugin.message_history[key], rows)

    async def test_negative_quantities_never_trigger_deletion(self):
        for method, text in (
            (self.plugin.batch_recall_command, "批量撤回 -1"),
            (self.plugin.recall_bot_messages_command, "撤回自身 -1"),
        ):
            event = self.event(text, reply=None)
            self.history(event)
            replies = await collect(method, event)
            self.assertIn("正整数", replies[0].chain[0].text)
        self.assertEqual(self.bot.api.calls, [])

    async def test_api_timeout_ends_batch_without_losing_history(self):
        event = self.event("批量撤回 2", reply=None)
        key, rows = self.history(event)
        self.bot.api.block = asyncio.Event()
        with self.assertLogs("batchrecall-test", "WARNING"):
            await asyncio.wait_for(
                collect(self.plugin.batch_recall_command, event), 0.3
            )
        self.assertEqual(len(self.bot.api.calls), 1)
        self.assertEqual(self.plugin.message_history[key], rows)
        self.assertTrue(event.is_stopped())

    async def test_external_cancel_propagates_and_releases_confirmation(self):
        self.bot.api.block = asyncio.Event()
        task = asyncio.create_task(collect(self.plugin.on_message, self.event()))
        await asyncio.sleep(0)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.plugin.confirmations.waiters, {})

    async def test_recall_notice_is_not_reinserted_as_history(self):
        event = self.event()
        key, _ = self.history(event)
        notice = {
            "post_type": "notice",
            "notice_type": "group_recall",
            "message_id": 11,
            "group_id": 123,
            "user_id": 100,
        }
        await self.bot.emit(notice)
        event.message_obj.raw_message = notice
        await collect(self.plugin.on_message, event)
        self.assertEqual([row[0] for row in self.plugin.message_history[key]], [12])

    async def test_list_numbers_survive_new_messages(self):
        listing = self.event("消息列表", reply=None)
        key, _ = self.history(listing)
        replies = await collect(self.plugin.message_list_command, listing)
        self.assertIn("[2] 用户: A", replies[0].chain[0].text)
        await listing.send(replies[0])
        self.plugin._add_message_to_history(key, 99, "new", "43", "新用户")
        request = self.event("批量撤回 2,", reply=None)
        await collect(self.plugin.batch_recall_command, request)
        self.assertEqual(self.bot.api.calls[-1], ("delete_msg", {"message_id": 11}))

    async def test_failed_list_delivery_cannot_replace_displayed_snapshot(self):
        listing = self.event("消息列表", reply=None)
        self.history(listing)
        replies = await collect(self.plugin.message_list_command, listing)
        self.bot.api.error = RuntimeError("send failed")
        with self.assertRaises(RuntimeError):
            await listing.send(replies[0])
        self.bot.api.error = None
        request = self.event("批量撤回 2,", reply=None)
        result = await collect(self.plugin.batch_recall_command, request)
        self.assertIn("请先发送", result[0].chain[0].text)
        self.assertEqual(len(self.bot.api.calls), 1)

    async def test_snapshot_is_scoped_to_its_requester(self):
        listing = self.event("消息列表", reply=None)
        self.history(listing)
        replies = await collect(self.plugin.message_list_command, listing)
        await listing.send(replies[0])
        request = self.event("批量撤回 2,", sender="43", reply=None)
        result = await collect(self.plugin.batch_recall_command, request)
        self.assertIn("请先发送", result[0].chain[0].text)
        self.assertEqual(len(self.bot.api.calls), 1)

    async def test_group_and_private_history_keys_do_not_collide(self):
        group = self.event(group="42")
        private = self.event(group="")
        self.assertNotEqual(
            self.plugin._get_session_id(group), self.plugin._get_session_id(private)
        )

    async def test_mentions_do_not_inject_qq_ids_into_number_arguments(self):
        listing = self.event("消息列表", reply=None)
        self.history(listing)
        replies = await collect(self.plugin.message_list_command, listing)
        await listing.send(replies[0])
        request = self.event("批量撤回 @用户7(42) 1,", reply=None)
        request.messages = [Plain("批量撤回 "), At(42), Plain(" 1,")]
        await collect(self.plugin.batch_recall_command, request)
        self.assertEqual(self.bot.api.calls[-1], ("delete_msg", {"message_id": 11}))

    async def test_reply_permission_denial_never_calls_platform(self):
        event = self.event()
        event.admin = False
        result = await collect(self.plugin.on_message, event)
        self.assertIn("仅管理员", result[0].chain[0].text)
        self.assertEqual(self.bot.api.calls, [])

    async def test_chat_and_unmatched_prefix_remain_eligible_for_llm(self):
        for text in ("你好", "#!不存在", "撤回是什么意思"):
            event = self.event(text, reply=None)
            await collect(self.plugin.on_message, event)
            self.assertFalse(event.call_llm)
            self.assertFalse(event.is_stopped())


if __name__ == "__main__":
    unittest.main()
