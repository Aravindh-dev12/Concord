"""
Baseline 2: Last-Write-Wins CRDT (NTP timestamp-based register per key).
Vulnerable to clock drift clobbering physical truth and zombie resurrections.
"""
from typing import List, Dict
from concord.engines.base import BaseMemoryEngine
from concord.models import BeliefRecord

class LWWCRDTEngine(BaseMemoryEngine):
    def __init__(self, node_id: str, is_cloud: bool = False):
        super().__init__(node_id, is_cloud)
        self.key_register: Dict[str, BeliefRecord] = {}
        self.outbound: List[BeliefRecord] = []

    async def ingest(self, record: BeliefRecord) -> bool:
        existing = self.key_register.get(record.belief_key)
        if not existing or record.created_at >= existing.created_at:
            self.key_register[record.belief_key] = record
            self.outbound.append(record)
            return True
        return False

    async def prepare_sync_batch(self, target_node_id: str, max_items: int = 100) -> List[BeliefRecord]:
        batch = self.outbound[:max_items]
        self.outbound = self.outbound[max_items:]
        return batch

    async def apply_sync_batch(self, records: List[BeliefRecord]) -> List[str]:
        applied = []
        for r in records:
            existing = self.key_register.get(r.belief_key)
            if not existing or r.created_at >= existing.created_at:
                self.key_register[r.belief_key] = r
                applied.append(r.id)
        return applied

    async def query(self, query_text: str, limit: int = 5) -> List[BeliefRecord]:
        matches = [
            r for r in self.key_register.values() 
            if (query_text.lower() in r.statement.lower() or query_text.lower() in r.belief_key.lower()) and not r.is_tombstone
        ]
        return matches[:limit]

    async def dump_all(self) -> List[BeliefRecord]:
        return [r for r in self.key_register.values() if not r.is_tombstone]

    async def get_tombstones(self) -> List[str]:
        return [r.id for r in self.key_register.values() if r.is_tombstone]