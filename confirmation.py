"""Correlate delete_msg requests with actual OneBot recall notices."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import contextmanager


class RecallConfirmations:
    def __init__(self) -> None:
        self.waiters: dict[tuple[int, int], set[asyncio.Future]] = defaultdict(set)

    @contextmanager
    def track(self, bot, message_id: int):
        key = (id(bot), message_id)
        future = asyncio.get_running_loop().create_future()
        self.waiters[key].add(future)
        try:
            yield future
        finally:
            pending = self.waiters.get(key)
            if pending is not None:
                pending.discard(future)
                if not pending:
                    self.waiters.pop(key, None)
            if not future.done():
                future.cancel()

    def confirm(self, bot, message_id: int) -> None:
        for future in self.waiters.get((id(bot), message_id), ()):
            if not future.done():
                future.set_result(None)

    def close(self) -> None:
        for pending in self.waiters.values():
            for future in pending:
                future.cancel()
        self.waiters.clear()
