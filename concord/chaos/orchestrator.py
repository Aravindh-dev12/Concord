"""
Chaos Orchestrator: Runs the tournament across all 4 competing engines.
"""
from typing import Dict, List, Type
from concord.engines.base import BaseMemoryEngine
from concord.engines.raw_queue import RawQueueEngine
from concord.engines.lww_crdt import LWWCRDTEngine
from concord.engines.latest_wins import LatestStatementWinsEngine
from concord.engines.concord_lite import ConcordLiteEngine
from concord.chaos.scenarios import ScenarioRunner
from concord.models import ScenarioResult

COMPETING_ENGINES: Dict[str, Type[BaseMemoryEngine]] = {
    "RawQueue": RawQueueEngine,
    "LWW-CRDT": LWWCRDTEngine,
    "LatestWins": LatestStatementWinsEngine,
    "ConcordLite": ConcordLiteEngine,
}

class ChaosTournamentOrchestrator:
    async def run_full_matrix(self) -> Dict[str, List[ScenarioResult]]:
        matrix_results: Dict[str, List[ScenarioResult]] = {}
        for engine_name, engine_cls in COMPETING_ENGINES.items():
            runner = ScenarioRunner(engine_cls)
            matrix_results[engine_name] = await runner.run_all()
        return matrix_results