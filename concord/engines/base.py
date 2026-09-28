"""
Abstract base class for all memory engines (Baselines and Concord-Lite).
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from concord.models import BeliefRecord

class BaseMemoryEngine(ABC):
    def __init__(self, node_id: str, is_cloud: bool = False):
        self.node_id = node_id
        self.is_cloud = is_cloud
        self.vector_clock: Dict[str, int] = {node_id: 0}

    @abstractmethod
    async def ingest(self, record: BeliefRecord) -> bool:
        """Ingests a new record locally into this node's state."""
        pass

    @abstractmethod
    async def prepare_sync_batch(self, target_node_id: str, max_items: int = 100) -> List[BeliefRecord]:
        """Packs records to be synchronized over network link."""
        pass

    @abstractmethod
    async def apply_sync_batch(self, records: List[BeliefRecord]) -> List[str]:
        """Applies incoming batch of records from another node."""
        pass

    @abstractmethod
    async def query(self, query_text: str, limit: int = 5) -> List[BeliefRecord]:
        """Executes semantic/key retrieval."""
        pass

    @abstractmethod
    async def dump_all(self) -> List[BeliefRecord]:
        """Returns all live (active) belief records stored in memory."""
        pass

    @abstractmethod
    async def get_tombstones(self) -> List[str]:
        """Returns list of tombstoned IDs."""
        pass