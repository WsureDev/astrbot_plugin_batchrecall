import asyncio
import unittest

from support import MessageChain, QQEvent, plugin


class DeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.plugin, self.bot, self.context = await plugin(automatic=True)

    async def asyncTearDown(self):
        await self.plugin.terminate()

    async def test_normal_send_keeps_chain_and_records_actual_receipt(self):
        event = QQEvent(bot=self.bot)
        chain = MessageChain().message("普通回复")
        await event.send(chain)
        self.assertEqual(chain.chain[0].text, "普通回复")
        self.assertTrue(event._has_send_oper)
        self.assertEqual(len(self.bot.api.calls), 1)
        self.assertEqual(self.plugin.message_history["test:group:123"][0][0], 901)
        self.assertEqual(len(self.plugin.recall_tasks), 1)

    async def test_command_exemption_does_not_cross_groups(self):
        command = QQEvent(group="123", bot=self.bot)
        normal = QQEvent(group="456", bot=self.bot)
        self.plugin._mark_no_auto_recall(command)
        await normal.send(MessageChain().message("普通回复"))
        await command.send(MessageChain().message("指令回执"))
        self.assertEqual(len(self.plugin.recall_tasks), 1)
        self.assertEqual(len(self.bot.api.calls), 2)

    async def test_send_exception_propagates_once_without_retry_or_history(self):
        self.bot.api.error = RuntimeError("send failed")
        with self.assertRaisesRegex(RuntimeError, "send failed"):
            await QQEvent(bot=self.bot).send(MessageChain().message("正文"))
        self.assertEqual(len(self.bot.api.calls), 1)
        self.assertEqual(self.plugin.message_history, {})
        self.assertEqual(self.plugin.recall_tasks, set())

    async def test_active_api_wrapper_preserves_result_object(self):
        result = {"message_id": 902}
        message = [{"type": "image", "data": {"file": "unchanged"}}]
        calls = []

        async def transport(action, **params):
            calls.append((action, params))
            return result

        observer = self.plugin.observers[id(self.bot)]
        observer.original = transport
        response = await self.bot.call_action(
            "send_group_msg", group_id=123, message=message
        )
        self.assertIs(response, result)
        self.assertIs(calls[0][1]["message"], message)
        self.assertEqual(len(calls), 1)

    async def test_proactive_sends_use_the_destination_channel(self):
        await self.context.send_message(
            "test:GroupMessage:456", MessageChain().message("后台结果")
        )
        self.assertIn("test:group:456", self.plugin.message_history)
        self.assertEqual(len(self.plugin.recall_tasks), 1)

    async def test_excluded_plugin_does_not_observe_channel(self):
        self.context.config["plugin_set"] = ["another_plugin"]
        await QQEvent(bot=self.bot).send(MessageChain().message("正文"))
        self.assertEqual(self.plugin.message_history, {})
        self.assertEqual(len(self.bot.api.calls), 1)

    async def test_event_plugin_exclusion_does_not_schedule_recall(self):
        event = QQEvent(bot=self.bot)
        event.plugins_name = ["another_plugin"]
        await event.send(MessageChain().message("正文"))
        self.assertEqual(self.plugin.message_history, {})
        self.assertEqual(len(self.bot.api.calls), 1)

    async def test_proactive_admin_private_chat_respects_admin_setting(self):
        self.context.config["admins_id"] = ["42"]
        await self.context.send_message(
            "test:FriendMessage:42", MessageChain().message("后台结果")
        )
        self.assertEqual(self.plugin.recall_tasks, set())
        self.assertIn("test:private:42", self.plugin.message_history)

    async def test_reloading_behind_another_wrapper_leaves_old_wrapper_inert(self):
        previous = self.bot.api.call_action

        async def later(action, **params):
            return await previous(action, **params)

        self.bot.api.call_action = later
        await self.plugin.terminate()
        self.assertIs(self.bot.api.call_action, later)
        self.assertTrue(
            all(not callbacks for callbacks in self.bot.subscribers.values())
        )
        before = dict(self.plugin.message_history)
        await self.bot.call_action("send_group_msg", group_id=123, message=[])
        self.assertEqual(self.plugin.message_history, before)

    async def test_shutdown_cancels_automatic_recall_waiters(self):
        await QQEvent(bot=self.bot).send(MessageChain().message("正文"))
        tasks = list(self.plugin.recall_tasks)
        await asyncio.sleep(0)
        await self.plugin.terminate()
        self.assertTrue(tasks[0].cancelled())
        self.assertEqual(self.plugin.recall_tasks, set())


if __name__ == "__main__":
    unittest.main()
