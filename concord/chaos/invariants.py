"""
The 8 Invariant Assertion Verifiers.
"""
from typing import List, Dict
from concord.engines.base import BaseMemoryEngine
from concord.models import InvariantResult, OutcomeStatus

class InvariantChecker:

    @staticmethod
    async def check_inv1_convergence(nodes: List[BaseMemoryEngine], query_key: str) -> InvariantResult:
        """INV-1: Strong Belief Convergence across all active nodes."""
        results = []
        for n in nodes:
            res = await n.query(query_key, limit=1)
            top_val = res[0].statement if res else None
            results.append((n.node_id, top_val))

        first_val = results[0][1]
        all_match = all(val == first_val and val is not None for _, val in results)
        
        return InvariantResult(
            check_id="INV-1",
            name="Strong Belief Convergence",
            passed=all_match,
            severity="Fatal",
            message="All nodes converged on identical belief" if all_match else f"Convergence diverged: {dict(results)}",
            details={"node_states": dict(results)}
        )

    @staticmethod
    async def check_inv2_epistemic_integrity(nodes: List[BaseMemoryEngine]) -> InvariantResult:
        """INV-2: Epistemic Integrity (UNKNOWN cannot have confidence >= 0.80 or be marked VERIFIED)."""
        violations = []
        for n in nodes:
            records = await n.dump_all()
            for r in records:
                if r.outcome_status == OutcomeStatus.UNKNOWN and r.confidence_score >= 0.80:
                    violations.append((n.node_id, r.id, r.confidence_score))

        passed = len(violations) == 0
        return InvariantResult(
            check_id="INV-2",
            name="Epistemic Integrity",
            passed=passed,
            severity="Fatal",
            message="No epistemic leaks found" if passed else f"Unverified claims shown with high confidence: {violations}",
            details={"violations": violations}
        )

    @staticmethod
    async def check_inv3_causal_monotonicity(records_observed: List[str], expected_order: List[str]) -> InvariantResult:
        """INV-3: Causal Monotonicity (Observed execution sequence conforms to causal lineage)."""
        passed = records_observed == expected_order
        return InvariantResult(
            check_id="INV-3",
            name="Causal Monotonicity",
            passed=passed,
            severity="Critical",
            message="Causal order preserved" if passed else f"Causal order violated. Expected {expected_order}, got {records_observed}",
            details={"observed": records_observed, "expected": expected_order}
        )

    @staticmethod
    async def check_inv4_no_phantom_resurrections(nodes: List[BaseMemoryEngine], tombstoned_id: str) -> InvariantResult:
        """INV-4: No Phantom Resurrections (Superseded/deleted record never re-enters live index)."""
        leaks = []
        for n in nodes:
            records = await n.dump_all()
            for r in records:
                if r.id == tombstoned_id:
                    leaks.append(n.node_id)
        passed = len(leaks) == 0
        return InvariantResult(
            check_id="INV-4",
            name="No Phantom Resurrections",
            passed=passed,
            severity="Fatal",
            message="Tombstoned record remained expunged" if passed else f"Zombie record resurfaced on nodes: {leaks}",
            details={"leaked_nodes": leaks, "target_id": tombstoned_id}
        )

    @staticmethod
    async def check_inv5_critical_latency(critical_arrived_first: bool) -> InvariantResult:
        """INV-5: Priority-Fair Sync (Critical safety beliefs arrive ahead of throttled bulk data)."""
        return InvariantResult(
            check_id="INV-5",
            name="Priority-Fair Ingestion",
            passed=critical_arrived_first,
            severity="Warning",
            message="High priority memory bypassed throttled bulk queue" if critical_arrived_first else "Critical record starved behind bulk FIFO queue",
            details={"critical_first": critical_arrived_first}
        )

    @staticmethod
    async def check_inv6_byzantine_isolation(cloud_node: BaseMemoryEngine, spoofed_id: str) -> InvariantResult:
        """INV-6: Byzantine Isolation (Unsigned or tampered payload rejected from memory)."""
        records = await cloud_node.dump_all()
        injected = any(r.id == spoofed_id for r in records)
        passed = not injected
        return InvariantResult(
            check_id="INV-6",
            name="Byzantine Isolation",
            passed=passed,
            severity="Fatal",
            message="Spoofed edge record was quarantined" if passed else "Malicious payload successfully polluted cloud memory",
            details={"spoofed_id": spoofed_id, "accepted": injected}
        )

    @staticmethod
    async def check_inv7_model_quarantine(cloud_node: BaseMemoryEngine, drifted_id: str) -> InvariantResult:
        """INV-7: Embedding Dimension & Namespace Quarantine."""
        records = await cloud_node.dump_all()
        drift_accepted = any(r.id == drifted_id for r in records)
        passed = not drift_accepted
        return InvariantResult(
            check_id="INV-7",
            name="Model Drift Quarantine",
            passed=passed,
            severity="Critical",
            message="Drifted embedding model quarantined" if passed else "Incompatible embedding indexed into store",
            details={"drifted_id": drifted_id, "indexed": drift_accepted}
        )

    @staticmethod
    async def check_inv8_zero_leakage_privacy(cloud_node: BaseMemoryEngine) -> InvariantResult:
        """INV-8: Zero-Leakage Privacy Boundary (is_private=True never enters central store)."""
        records = await cloud_node.dump_all()
        private_leaks = [r.id for r in records if r.is_private]
        passed = len(private_leaks) == 0
        return InvariantResult(
            check_id="INV-8",
            name="Zero-Leakage Privacy",
            passed=passed,
            severity="Fatal",
            message="No private records leaked across network boundary" if passed else f"Private records detected in cloud index: {private_leaks}",
            details={"leaked_ids": private_leaks}
        )