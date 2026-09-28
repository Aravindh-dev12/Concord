"""
Concord Enterprise Server: Exposes REST APIs for Ledger, Sync, Agent, and Chaos Lab.
"""
import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Dict, List
from concord.models import BeliefRecord, OutcomeStatus, Provenance
from concord.crypto import GLOBAL_KEYRING
from concord.engines.concord_lite import ConcordLiteEngine
from concord.chaos.orchestrator import ChaosTournamentOrchestrator
from concord.agent import EdgeFieldAgent
from concord.policy import SyncPolicyEngine, NetworkLinkProfile

app = FastAPI(title="Concord: Edge Memory Platform", version="2.0.0")

ACTIVE_NODES: Dict[str, ConcordLiteEngine] = {}
AGENTS: Dict[str, EdgeFieldAgent] = {}
POLICIES: Dict[str, SyncPolicyEngine] = {}

def get_node(node_id: str) -> ConcordLiteEngine:
    if node_id not in ACTIVE_NODES:
        GLOBAL_KEYRING.register_device(node_id)
        node = ConcordLiteEngine(node_id=node_id, is_cloud=(node_id == "cloud"))
        ACTIVE_NODES[node_id] = node
        AGENTS[node_id] = EdgeFieldAgent(node=node)
        POLICIES[node_id] = SyncPolicyEngine(node_id=node_id)
    return ACTIVE_NODES[node_id]

# Pre-populate edge nodes and cloud store
get_node("edge_a")
get_node("edge_b")
get_node("cloud")

orchestrator = ChaosTournamentOrchestrator()
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

class IngestRequest(BaseModel):
    belief_key: str
    statement: str
    outcome_status: OutcomeStatus = OutcomeStatus.VERIFIED
    criticality: float = 0.5
    is_private: bool = False

class SearchRequest(BaseModel):
    query: str
    limit: int = 5

class SyncRequest(BaseModel):
    src_node: str
    dst_node: str

class AgentTriggerRequest(BaseModel):
    sensor_key: str
    reading_value: float
    threshold: float = 90.0

class LinkPolicyRequest(BaseModel):
    profile: NetworkLinkProfile

@app.get("/")
async def root():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))

@app.get("/api/nodes")
async def list_nodes():
    return {"nodes": list(ACTIVE_NODES.keys())}

@app.get("/api/stats/{node_id}")
async def get_stats(node_id: str):
    node = get_node(node_id)
    return node.vector_store.get_stats()

@app.get("/api/ledger/{node_id}")
async def get_ledger(node_id: str):
    node = get_node(node_id)
    records = await node.dump_all()
    return {"node_id": node_id, "records": [r.dict() for r in records]}

@app.post("/api/ledger/{node_id}/add")
async def add_belief(node_id: str, req: IngestRequest):
    node = get_node(node_id)
    payload = f"{req.belief_key}:{req.statement}".encode("utf-8")
    sig = GLOBAL_KEYRING.sign(node_id, payload)
    
    rec = BeliefRecord(
        belief_key=req.belief_key,
        statement=req.statement,
        outcome_status=req.outcome_status,
        criticality=req.criticality,
        is_private=req.is_private,
        provenance=Provenance(origin_device_id=node_id, signature=sig)
    )
    await node.ingest(rec)
    return {"status": "ok", "record": rec.dict()}

@app.post("/api/ledger/{node_id}/search")
async def search_ledger(node_id: str, req: SearchRequest):
    node = get_node(node_id)
    results = await node.hybrid_search_with_scores(req.query, limit=req.limit)
    return {
        "query": req.query,
        "results": [{"record": r.dict(), "score": round(score, 4)} for r, score in results]
    }

@app.post("/api/sync")
async def sync_nodes(req: SyncRequest):
    src = get_node(req.src_node)
    dst = get_node(req.dst_node)
    policy = POLICIES.get(req.src_node)
    
    raw_batch = await src.prepare_sync_batch(req.dst_node)
    eligible_records = []
    
    for record in raw_batch:
        allowed, _ = policy.evaluate_sync_eligibility(record)
        if allowed:
            eligible_records.append(record)

    applied = await dst.apply_sync_batch(eligible_records)
    return {
        "status": "ok",
        "transferred": len(eligible_records),
        "applied": len(applied),
        "applied_ids": applied,
        "policy_active": policy.current_link.value
    }

@app.get("/api/ledger/{node_id}/quarantine")
async def get_quarantine(node_id: str):
    node = get_node(node_id)
    return {"node_id": node_id, "quarantine": node.quarantine}

@app.post("/api/agent/{node_id}/trigger")
async def trigger_agent(node_id: str, req: AgentTriggerRequest):
    get_node(node_id)
    agent = AGENTS[node_id]
    result = await agent.evaluate_field_telemetry(req.sensor_key, req.reading_value, req.threshold)
    return result

@app.post("/api/policy/{node_id}")
async def set_node_policy(node_id: str, req: LinkPolicyRequest):
    get_node(node_id)
    POLICIES[node_id].set_link_profile(req.profile)
    return {"node_id": node_id, "profile": req.profile.value}

@app.post("/api/chaos/run-matrix")
async def run_matrix():
    results = await orchestrator.run_full_matrix()
    return {"status": "complete", "matrix": results}