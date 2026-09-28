"""
Baseline 1: Naive FIFO Queue (Qdrant docs style standard sync).
Suffers from: Starvation, clock drift, zombie resurrection, and private leaks.
"""
from typing import List, Dict
import numpy as np
from concord.engines.base import BaseMemoryEngine
from concord.models import BeliefRecord

class RawQueueEngine(BaseMemoryEngine):
    def __init__(self, node_id: str, is_cloud: bool = False):
        super().__init__(node_id, is_cloud)
        self.store: Dict[str, BeliefRecord] = {}
        self.outbound_queue: List[BeliefRecord] = []

    async def ingest(self, record: BeliefRecord) -> bool:
        self.store[record.id] = record
        self.outbound_queue.append(record)
        return True

    async def prepare_sync_batch(self, target_node_id: str, max_items: int = 100) -> List[BeliefRecord]:
        # Naive FIFO: pops whatever is oldest
        batch = self.outbound_queue[:max_items]
        self.outbound_queue = self.outbound_queue[max_items:]
        return batch

    async def apply_sync_batch(self, records: List[BeliefRecord]) -> List[str]:
        accepted = []
        for r in records:
            self.store[r.id] = r
            accepted.append(r.id)
        return accepted

    async def query(self, query_text: str, limit: int = 5) -> List[BeliefRecord]:
        # Filter matching text or key
        matches = [r for r in self.store.values() if query_text.lower() in r.statement.lower() or query_text.lower() in r.belief_key.lower()]
        return matches[:limit]

    async def dump_all(self) -> List[BeliefRecord]:
        return list(self.store.values())

    async def get_tombstones(self) -> List[str]:
        return []