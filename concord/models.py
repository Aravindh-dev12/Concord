from __future__ import annotations
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import time
import uuid

class OutcomeStatus(str, Enum):
    VERIFIED = "VERIFIED"
    UNKNOWN = "UNKNOWN"
    REFUTED = "REFUTED"

class Provenance(BaseModel):
    origin_device_id: str
    signature: str
    witness_chain: List[str] = Field(default_factory=list)

class BeliefRecord(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    belief_key: str
    statement: str
    embedding: List[float] = Field(default_factory=list)
    model_fingerprint: str = "text-embedding-3-small:v1"
    outcome_status: OutcomeStatus = OutcomeStatus.UNKNOWN
    confidence_score: float = 0.5
    criticality: float = 0.1  # 0.0 to 1.0 (for priority scheduling)
    provenance: Provenance
    crdt_vector_clock: Dict[str, int] = Field(default_factory=dict)
    is_private: bool = False
    is_tombstone: bool = False
    supersedes: List[str] = Field(default_factory=list)
    created_at: float = Field(default_factory=lambda: time.time())

    def causal_dominates(self, other: BeliefRecord) -> bool:
        keys = set(self.crdt_vector_clock.keys()).union(other.crdt_vector_clock.keys())
        greater_or_equal = True
        strictly_greater = False
        for k in keys:
            v_self = self.crdt_vector_clock.get(k, 0)
            v_other = other.crdt_vector_clock.get(k, 0)
            if v_self < v_other:
                greater_or_equal = False
            if v_self > v_other:
                strictly_greater = True
        return greater_or_equal and strictly_greater

class InvariantResult(BaseModel):
    check_id: str
    name: str
    passed: bool
    severity: str
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)

class ScenarioResult(BaseModel):
    scenario_id: str
    scenario_name: str
    engine_name: str
    success: bool
    duration_ms: float
    invariants: List[InvariantResult]
    logs: List[str]