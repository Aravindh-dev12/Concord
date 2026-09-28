"""
Target Reference Engine: Concord-Lite
Features:
- Outcome-Ruled Merge (VERIFIED > UNKNOWN, Vector Clock precedence).
- Deterministic Concurrent Tie-Breaking (prevents split-brain divergence).
- Outbound Peer Relay & Per-Peer Sync Tracking.
- Priority-Scored Sync (Critical records synced first).
- Cryptographic Signature & Model Fingerprint Quarantine.
- Zero-Leakage Privacy Boundary (private records stripped before egress).
- Immutable Tombstone Log (prevents zombie resurrections).
"""
from typing import List, Dict, Set, Any
from concord.engines.base import BaseMemoryEngine
from concord.models import BeliefRecord, OutcomeStatus
from concord.crypto import GLOBAL_KEYRING

class ConcordLiteEngine(BaseMemoryEngine):
    SUPPORTED_MODEL_FINGERPRINT = "text-embedding-3-small:v1"

    def __init__(self, node_id: str, is_cloud: bool = False):
        super().__init__(node_id, is_cloud)
        self.store: Dict[str, BeliefRecord] = {}
        self.active_key_map: Dict[str, str] = {}  # belief_key -> record_id
        self.tombstones: Set[str] = set()
        self.quarantine: Dict[str, Dict[str, Any]] = {}
        self.outbound_priority_queue: List[BeliefRecord] = []
        self._sent_to_peers: Dict[str, Set[str]] = {}

    def _increment_clock(self):
        self.vector_clock[self.node_id] = self.vector_clock.get(self.node_id, 0) + 1

    async def ingest(self, record: BeliefRecord) -> bool:
        self._increment_clock()
        record.crdt_vector_clock[self.node_id] = self.vector_clock[self.node_id]
        
        # Outcome Rule: UNKNOWN records cap confidence at 0.49
        if record.outcome_status == OutcomeStatus.UNKNOWN:
            record.confidence_score = min(record.confidence_score, 0.49)
        elif record.outcome_status == OutcomeStatus.VERIFIED:
            record.confidence_score = max(record.confidence_score, 0.85)

        self.store[record.id] = record
        
        if record.is_tombstone:
            self.tombstones.add(record.id)
            for superseded in record.supersedes:
                self.tombstones.add(superseded)
            if record.belief_key in self.active_key_map:
                del self.active_key_map[record.belief_key]
        else:
            self.active_key_map[record.belief_key] = record.id

        self.outbound_priority_queue.append(record)
        return True

    async def prepare_sync_batch(self, target_node_id: str, max_items: int = 100) -> List[BeliefRecord]:
        if target_node_id not in self._sent_to_peers:
            self._sent_to_peers[target_node_id] = set()

        already_sent = self._sent_to_peers[target_node_id]

        # 1. Filter out Private records, echo records back to their creator, and already-sent records
        eligible = [
            r for r in self.outbound_priority_queue 
            if not r.is_private 
            and r.id not in already_sent 
            and r.provenance.origin_device_id != target_node_id
        ]
        
        # 2. Priority Ordering (Criticality 1.0 syncs before bulk 0.1)
        eligible.sort(key=lambda r: (r.criticality, r.created_at), reverse=True)
        
        batch = eligible[:max_items]
        for r in batch:
            already_sent.add(r.id)

        return batch

    async def apply_sync_batch(self, records: List[BeliefRecord]) -> List[str]:
        applied = []
        for r in records:
            # 1. Byzantine Check: Verify Signature
            payload_to_verify = f"{r.belief_key}:{r.statement}".encode("utf-8")
            if not GLOBAL_KEYRING.verify(r.provenance.origin_device_id, payload_to_verify, r.provenance.signature):
                self.quarantine[r.id] = {"record": r, "reason": "INVALID_SIGNATURE"}
                continue

            # 2. Embedding Model / Namespace Quarantine
            if r.model_fingerprint != self.SUPPORTED_MODEL_FINGERPRINT:
                self.quarantine[r.id] = {"record": r, "reason": "MODEL_DRIFT_MISMATCH"}
                continue

            # 3. Tombstone Check (Prevent Phantom Resurrections)
            if r.id in self.tombstones or any(sup in self.tombstones for sup in r.supersedes):
                continue

            # 4. Outcome-Ruled & Causal Dominance Conflict Resolution
            current_active_id = self.active_key_map.get(r.belief_key)
            if current_active_id:
                current_record = self.store[current_active_id]
                
                # Rule A: VERIFIED beats UNKNOWN
                if current_record.outcome_status == OutcomeStatus.VERIFIED and r.outcome_status == OutcomeStatus.UNKNOWN:
                    continue  # Reject unverified downgrade
                
                # If current is UNKNOWN and incoming is VERIFIED, incoming always supersedes
                if current_record.outcome_status == OutcomeStatus.UNKNOWN and r.outcome_status == OutcomeStatus.VERIFIED:
                    pass
                else:
                    # Rule B: Vector Clock causal dominance
                    if current_record.causal_dominates(r):
                        continue
                    
                    # Rule C: Deterministic tie-breaker for concurrent updates
                    # (When neither dominates and both have equal epistemic status)
                    if not r.causal_dominates(current_record):
                        current_tie = (
                            current_record.confidence_score, 
                            current_record.provenance.origin_device_id, 
                            current_record.id
                        )
                        incoming_tie = (
                            r.confidence_score, 
                            r.provenance.origin_device_id, 
                            r.id
                        )
                        if current_tie >= incoming_tie:
                            continue  # Current record deterministically wins; drop incoming

            # Merge clocks
            for k, v in r.crdt_vector_clock.items():
                self.vector_clock[k] = max(self.vector_clock.get(k, 0), v)

            self.store[r.id] = r
            if r.is_tombstone:
                self.tombstones.add(r.id)
                if r.belief_key in self.active_key_map:
                    del self.active_key_map[r.belief_key]
            else:
                self.active_key_map[r.belief_key] = r.id
                
            applied.append(r.id)

            # Relay to other connected peers in the next sync pass
            if not r.is_private:
                self.outbound_priority_queue.append(r)

        return applied

    async def query(self, query_text: str, limit: int = 5) -> List[BeliefRecord]:
        matches = []
        for r_id in self.active_key_map.values():
            r = self.store.get(r_id)
            if r and not r.is_tombstone and r_id not in self.tombstones:
                if query_text.lower() in r.statement.lower() or query_text.lower() in r.belief_key.lower():
                    matches.append(r)
        return matches[:limit]

    async def dump_all(self) -> List[BeliefRecord]:
        return [self.store[r_id] for r_id in self.active_key_map.values() if r_id not in self.tombstones]

    async def get_tombstones(self) -> List[str]:
        return list(self.tombstones)