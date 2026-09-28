"""
Simulated Network Transport with Fault Injection:
Latency, Jitter, Packet Drop, Bandwidth Throttling, and Hard Partitions.
"""
import asyncio
import random
from typing import Dict, Set, List
from concord.models import BeliefRecord

class SimulatedNetwork:
    def __init__(self):
        self.partitions: Set[tuple] = set()
        self.throttles: Dict[str, float] = {}  # node_id -> delay in seconds
        self.packet_loss: Dict[str, float] = {} # node_id -> loss probability (0.0 to 1.0)

    def partition(self, node_a: str, node_b: str):
        self.partitions.add((node_a, node_b))
        self.partitions.add((node_b, node_a))

    def heal_partition(self, node_a: str, node_b: str):
        self.partitions.discard((node_a, node_b))
        self.partitions.discard((node_b, node_a))

    def heal_all(self):
        self.partitions.clear()
        self.throttles.clear()
        self.packet_loss.clear()

    def throttle(self, node_id: str, delay_sec: float):
        self.throttles[node_id] = delay_sec

    def set_packet_loss(self, node_id: str, probability: float):
        self.packet_loss[node_id] = probability

    def is_partitioned(self, src: str, dst: str) -> bool:
        return (src, dst) in self.partitions

    async def transmit(self, src: str, dst: str, records: List[BeliefRecord]) -> List[BeliefRecord]:
        if self.is_partitioned(src, dst):
            return []
        
        loss_prob = self.packet_loss.get(src, 0.0)
        if loss_prob > 0 and random.random() < loss_prob:
            return []

        delay = self.throttles.get(src, 0.0)
        if delay > 0:
            await asyncio.sleep(delay)

        return records