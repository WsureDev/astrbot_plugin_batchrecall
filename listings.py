"""Keep displayed message numbers bound to their original message IDs."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass

HistoryRow = tuple[int, str, int, str, str, bool]


@dataclass(frozen=True)
class PendingListing:
    owner: str
    rows: tuple[HistoryRow, ...]
    bot_id: str
    bot_name: str


class Listings:
    def __init__(self) -> None:
        self.snapshots: OrderedDict[tuple[str, str], tuple[HistoryRow, ...]] = (
            OrderedDict()
        )

    def remember(self, session: str, pending: PendingListing, message_id: int) -> None:
        key = (session, pending.owner)
        rows = ((message_id, "[消息列表]", 0, pending.bot_id, pending.bot_name, True),)
        self.snapshots[key] = rows + pending.rows
        self.snapshots.move_to_end(key)
        while len(self.snapshots) > 256:
            self.snapshots.popitem(last=False)

    def get(self, session: str, sender: str) -> tuple[HistoryRow, ...] | None:
        return self.snapshots.get((session, sender))
