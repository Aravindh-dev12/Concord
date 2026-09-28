"""
Baseline 3: Latest Statement Wins (Blind Key-Value Overwrite).
"""
from typing import List, Dict
from concord.engines.base import BaseMemoryEngine
from concord.models import BeliefRecord

class LatestStatementWinsEngine(BaseMemoryEngine):
    def __init__(self, node_id: str, is_cloud: bool = False):
        super().__init__(node_id, is_cloud)
        self.beliefs: Dict[str, BeliefRecord] = {}
        self.outbound: List[BeliefRecord] = []

    async def ingest(self, record: BeliefRecord) -> bool:
        self.beliefs[record.belief_key] = record
        self.outbound.append(record)
        return True

    async def prepare_sync_batch(self, target_node_id: str, max_items: int = 100) -> List[BeliefRecord]:
        batch = self.outbound[:max_items]
        self.outbound = self.outbound[max_items:]
        return batch

    async def apply_sync_batch(self, records: List[BeliefRecord]) -> List[str]:
        for r in records:
            self.beliefs[r.belief_key] = r
        return [r.id for r in records]

    async def query(self, query_text: str, limit: int = 5) -> List[BeliefRecord]:
        matches = [r for r in self.beliefs.values() if query_text.lower() in r.statement.lower() or query_text.lower() in r.belief_key.lower()]
        return matches[:limit]

    async def dump_all(self) -> List[BeliefRecord]:
        return list(self.beliefs.values())

    async def get_tombstones(self) -> List[str]:
        return []