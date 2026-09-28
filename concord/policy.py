"""
Dynamic synchronization and data residency policy engine for Concord Edge nodes.
"""
from enum import Enum
from typing import Dict, Any, List
from concord.models import BeliefRecord, OutcomeStatus

class NetworkLinkProfile(str, Enum):
    OFFLINE = "OFFLINE"
    LOW_BANDWIDTH = "LOW_BANDWIDTH"   # e.g., Satellite, LoRa, 2G (Throttled, packet drops)
    BROADBAND = "BROADBAND"           # e.g., Wi-Fi, 5G, Hardwire

class SyncPolicyEngine:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.current_link = NetworkLinkProfile.BROADBAND

    def set_link_profile(self, profile: NetworkLinkProfile):
        self.current_link = profile

    def evaluate_sync_eligibility(self, record: BeliefRecord) -> tuple[bool, str]:
        """
        Dynamically determines whether a record can leave the edge node based on:
        1. Zero-Leakage Privacy Policy
        2. Outcome Epistemic Status (VERIFIED vs UNKNOWN)
        3. Network Link Capacity and Record Criticality
        """
        # Rule 1: Strict Zero-Leakage Privacy Boundary
        if record.is_private:
            return False, "POLICY_REJECT_PRIVATE_DATA"

        # Rule 2: Complete link blackout
        if self.current_link == NetworkLinkProfile.OFFLINE:
            return False, "NETWORK_OFFLINE"

        # Rule 3: Low-Bandwidth / Satellite Constraint
        if self.current_link == NetworkLinkProfile.LOW_BANDWIDTH:
            # Under severe throttle, ONLY VERIFIED high-criticality events sync
            if record.criticality < 0.7:
                return False, "POLICY_DEFERRED_LOW_PRIORITY_FOR_BANDWIDTH"
            if record.outcome_status == OutcomeStatus.UNKNOWN:
                return False, "POLICY_DEFERRED_UNVERIFIED_FOR_BANDWIDTH"

        # Rule 4: Broadband / Full Sync
        return True, "ELIGIBLE_FOR_SYNC"