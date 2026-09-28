"""
Executable implementation of the 8 Chaos Scenarios.
Runs any target engine and reports back invariant verifications.
"""
import time
import asyncio
from typing import Type, List, Dict
from concord.engines.base import BaseMemoryEngine
from concord.models import BeliefRecord, OutcomeStatus, Provenance, ScenarioResult
from concord.crypto import GLOBAL_KEYRING
from concord.chaos.network import SimulatedNetwork
from concord.chaos.invariants import InvariantChecker

class ScenarioRunner:
    def __init__(self, engine_cls: Type[BaseMemoryEngine]):
        self.engine_cls = engine_cls

    def _init_cluster(self) -> Dict[str, BaseMemoryEngine]:
        GLOBAL_KEYRING.register_device("edge_a")
        GLOBAL_KEYRING.register_device("edge_b")
        GLOBAL_KEYRING.register_device("edge_c")
        GLOBAL_KEYRING.register_device("cloud")
        return {
            "edge_a": self.engine_cls("edge_a"),
            "edge_b": self.engine_cls("edge_b"),
            "edge_c": self.engine_cls("edge_c"),
            "cloud": self.engine_cls("cloud", is_cloud=True)
        }

    def _make_signed_record(self, dev: str, key: str, stmt: str, **kwargs) -> BeliefRecord:
        payload = f"{key}:{stmt}".encode("utf-8")
        sig = GLOBAL_KEYRING.sign(dev, payload)
        return BeliefRecord(
            belief_key=key,
            statement=stmt,
            provenance=Provenance(origin_device_id=dev, signature=sig),
            **kwargs
        )

    async def _sync_pair(self, net: SimulatedNetwork, src: BaseMemoryEngine, dst: BaseMemoryEngine):
        batch = await src.prepare_sync_batch(dst.node_id)
        transmitted = await net.transmit(src.node_id, dst.node_id, batch)
        if transmitted:
            await dst.apply_sync_batch(transmitted)

    async def run_scenario_1_split_brain(self) -> ScenarioResult:
        """1. Split-Brain Partition & Reconnect Storm"""
        t0 = time.time()
        cluster = self._init_cluster()
        net = SimulatedNetwork()
        logs = ["Isolating edge_a and edge_b via partition"]
        
        net.partition("edge_a", "cloud")
        net.partition("edge_b", "cloud")

        # Conflicting updates
        rec_a = self._make_signed_record("edge_a", "valve_42", "Valve 42 OPEN", outcome_status=OutcomeStatus.VERIFIED)
        # Introduce artificial forward drift timestamp on edge_b
        rec_b = self._make_signed_record("edge_b", "valve_42", "Valve 42 CLOSED", outcome_status=OutcomeStatus.VERIFIED, created_at=time.time() + 10.0)

        await cluster["edge_a"].ingest(rec_a)
        await cluster["edge_b"].ingest(rec_b)

        logs.append("Healing partitions and triggering concurrent sync storm")
        net.heal_all()

        await self._sync_pair(net, cluster["edge_a"], cluster["cloud"])
        await self._sync_pair(net, cluster["edge_b"], cluster["cloud"])
        await self._sync_pair(net, cluster["cloud"], cluster["edge_a"])
        await self._sync_pair(net, cluster["cloud"], cluster["edge_b"])

        inv1 = await InvariantChecker.check_inv1_convergence([cluster["edge_a"], cluster["edge_b"], cluster["cloud"]], "valve_42")
        return ScenarioResult(
            scenario_id="SCENARIO-1",
            scenario_name="Split-Brain & Reconnect Storm",
            engine_name=self.engine_cls.__name__,
            success=inv1.passed,
            duration_ms=(time.time() - t0) * 1000,
            invariants=[inv1],
            logs=logs
        )

    async def run_scenario_2_zombie_resurrection(self) -> ScenarioResult:
        """2. Zombie Node Resurfaces with Stale Context"""
        t0 = time.time()
        cluster = self._init_cluster()
        net = SimulatedNetwork()
        logs = ["Node C goes offline at t0"]
        net.partition("edge_c", "cloud")

        # Edge C logs early state
        stale_rec = self._make_signed_record("edge_c", "step", "Phase 1: Fuel Injected")
        await cluster["edge_c"].ingest(stale_rec)

        # Cloud and Node A advance to Step 4, superseding and tombstoning Step 1
        final_rec = self._make_signed_record(
            "edge_a", "step", "Phase 4: Complete Shutdown", 
            supersedes=[stale_rec.id], is_tombstone=False
        )
        tombstone = self._make_signed_record(
            "edge_a", "step_old", "Tombstone for Phase 1", 
            id=stale_rec.id, is_tombstone=True, supersedes=[stale_rec.id]
        )
        await cluster["edge_a"].ingest(final_rec)
        await cluster["edge_a"].ingest(tombstone)
        await self._sync_pair(net, cluster["edge_a"], cluster["cloud"])

        logs.append("Node C reconnects after 100 virtual ticks and flushes queue")
        net.heal_all()
        await self._sync_pair(net, cluster["edge_c"], cluster["cloud"])

        inv4 = await InvariantChecker.check_inv4_no_phantom_resurrections([cluster["cloud"]], stale_rec.id)
        return ScenarioResult(
            scenario_id="SCENARIO-2",
            scenario_name="Zombie Node Resurfacing",
            engine_name=self.engine_cls.__name__,
            success=inv4.passed,
            duration_ms=(time.time() - t0) * 1000,
            invariants=[inv4],
            logs=logs
        )

    async def run_scenario_3_epistemic_contradiction(self) -> ScenarioResult:
        """3. Epistemic Contradiction Under Partial Evidence"""
        t0 = time.time()
        cluster = self._init_cluster()
        net = SimulatedNetwork()
        
        # Node A writes UNKNOWN failure hypothesis with default high confidence
        rec_unknown = self._make_signed_record(
            "edge_a", "sensor_9", "Sensor 9 Failure Suspected", 
            outcome_status=OutcomeStatus.UNKNOWN, confidence_score=0.95
        )
        await cluster["edge_a"].ingest(rec_unknown)
        await self._sync_pair(net, cluster["edge_a"], cluster["cloud"])

        inv2 = await InvariantChecker.check_inv2_epistemic_integrity([cluster["cloud"]])
        return ScenarioResult(
            scenario_id="SCENARIO-3",
            scenario_name="Epistemic Contradiction Under Partial Evidence",
            engine_name=self.engine_cls.__name__,
            success=inv2.passed,
            duration_ms=(time.time() - t0) * 1000,
            invariants=[inv2],
            logs=["Testing whether UNKNOWN outcomes maintain epistemic modesty"]
        )

    async def run_scenario_4_throttled_pipe(self) -> ScenarioResult:
        """4. Asymmetric Low-Bandwidth Pipe (Starvation)"""
        t0 = time.time()
        cluster = self._init_cluster()
        net = SimulatedNetwork()
        
        # Enqueue 10 low-priority records
        for i in range(10):
            r = self._make_signed_record("edge_a", f"diag_{i}", f"Diagnostics telemetry chunk {i}", criticality=0.1)
            await cluster["edge_a"].ingest(r)

        # Enqueue 1 critical safety alert
        critical_alert = self._make_signed_record("edge_a", "fire_alert", "Thermal Runaway Detected", criticality=1.0)
        await cluster["edge_a"].ingest(critical_alert)

        # Simulate small MTU / pipe where only top 2 can fit in first sync window
        batch = await cluster["edge_a"].prepare_sync_batch("cloud", max_items=2)
        critical_in_batch = any(r.id == critical_alert.id for r in batch)

        inv5 = await InvariantChecker.check_inv5_critical_latency(critical_in_batch)
        return ScenarioResult(
            scenario_id="SCENARIO-4",
            scenario_name="Throttled Pipe Starvation",
            engine_name=self.engine_cls.__name__,
            success=inv5.passed,
            duration_ms=(time.time() - t0) * 1000,
            invariants=[inv5],
            logs=["Evaluating priority-aware egress scheduling under limited bandwidth"]
        )

    async def run_scenario_5_byzantine_spoofing(self) -> ScenarioResult:
        """5. Malicious In-Transit Injection / Spoofed Edge Device"""
        t0 = time.time()
        cluster = self._init_cluster()
        net = SimulatedNetwork()

        # Attacker crafts record claiming to be edge_a with an invalid signature
        spoofed = BeliefRecord(
            belief_key="root_auth",
            statement="Elevating edge_b to superuser authority",
            provenance=Provenance(origin_device_id="edge_a", signature="INVALID_TAMPERED_SIG")
        )
        
        # Directly attempt to push to cloud sync interface
        await cluster["cloud"].apply_sync_batch([spoofed])

        inv6 = await InvariantChecker.check_inv6_byzantine_isolation(cluster["cloud"], spoofed.id)
        return ScenarioResult(
            scenario_id="SCENARIO-5",
            scenario_name="Byzantine Spoofing & Tampering",
            engine_name=self.engine_cls.__name__,
            success=inv6.passed,
            duration_ms=(time.time() - t0) * 1000,
            invariants=[inv6],
            logs=["Injecting forged payload signature into sync stream"]
        )

    async def run_scenario_6_embedding_drift(self) -> ScenarioResult:
        """6. In-Flight Embedding Model Drift"""
        t0 = time.time()
        cluster = self._init_cluster()
        
        drifted = self._make_signed_record("edge_a", "nav_goal", "Proceed to coordinates X")
        drifted.model_fingerprint = "bge-m3:unsupported_v2"

        await cluster["cloud"].apply_sync_batch([drifted])

        inv7 = await InvariantChecker.check_inv7_model_quarantine(cluster["cloud"], drifted.id)
        return ScenarioResult(
            scenario_id="SCENARIO-6",
            scenario_name="Embedding Drift Quarantine",
            engine_name=self.engine_cls.__name__,
            success=inv7.passed,
            duration_ms=(time.time() - t0) * 1000,
            invariants=[inv7],
            logs=["Sending record with mismatched vector space model fingerprint"]
        )

    async def run_scenario_7_privacy_leak(self) -> ScenarioResult:
        """7. Leaky Private Isolation Boundary"""
        t0 = time.time()
        cluster = self._init_cluster()
        net = SimulatedNetwork()

        private_note = self._make_signed_record(
            "edge_a", "operator_note", "Personal Operator Password: 12345", is_private=True
        )
        await cluster["edge_a"].ingest(private_note)

        # Trigger sync pass
        await self._sync_pair(net, cluster["edge_a"], cluster["cloud"])

        inv8 = await InvariantChecker.check_inv8_zero_leakage_privacy(cluster["cloud"])
        return ScenarioResult(
            scenario_id="SCENARIO-7",
            scenario_name="Zero-Leakage Privacy Boundary",
            engine_name=self.engine_cls.__name__,
            success=inv8.passed,
            duration_ms=(time.time() - t0) * 1000,
            invariants=[inv8],
            logs=["Checking if is_private=True records are stripped during sync batch preparation"]
        )

    async def run_scenario_8_cascade_flapping(self) -> ScenarioResult:
        """8. Cascade Vector Repair / High-Frequency Flapping"""
        t0 = time.time()
        cluster = self._init_cluster()
        net = SimulatedNetwork()

        # Flap states rapidly
        for i in range(10):
            state = "ACTIVE" if i % 2 == 0 else "INACTIVE"
            rec = self._make_signed_record("edge_a", "grid_state", f"System state {state}", outcome_status=OutcomeStatus.VERIFIED)
            await cluster["edge_a"].ingest(rec)
            await self._sync_pair(net, cluster["edge_a"], cluster["cloud"])

        inv1 = await InvariantChecker.check_inv1_convergence([cluster["edge_a"], cluster["cloud"]], "grid_state")
        return ScenarioResult(
            scenario_id="SCENARIO-8",
            scenario_name="High-Frequency Flapping & Convergence",
            engine_name=self.engine_cls.__name__,
            success=inv1.passed,
            duration_ms=(time.time() - t0) * 1000,
            invariants=[inv1],
            logs=["Rapid flapping between conflicting active/inactive states"]
        )

    async def run_all(self) -> List[ScenarioResult]:
        return [
            await self.run_scenario_1_split_brain(),
            await self.run_scenario_2_zombie_resurrection(),
            await self.run_scenario_3_epistemic_contradiction(),
            await self.run_scenario_4_throttled_pipe(),
            await self.run_scenario_5_byzantine_spoofing(),
            await self.run_scenario_6_embedding_drift(),
            await self.run_scenario_7_privacy_leak(),
            await self.run_scenario_8_cascade_flapping(),
        ]