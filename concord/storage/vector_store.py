"""
On-disk embedded Qdrant vector store per edge node.
"""
import os
import uuid
from typing import List, Dict, Any, Tuple
from concord.models import BeliefRecord
from concord.embeddings import LocalEdgeEmbeddingEngine

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
    HAS_QDRANT = True
except ImportError:
    HAS_QDRANT = False


class EdgeQdrantVectorStore:
    COLLECTION_NAME = "concord_edge_memory"

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.vector_dim = LocalEdgeEmbeddingEngine.DIMENSION
        self.storage_path = os.path.abspath(f"./qdrant_storage_{node_id}")
        self._fallback_records: Dict[str, BeliefRecord] = {}
        self.client = None

        if HAS_QDRANT:
            try:
                os.makedirs(self.storage_path, exist_ok=True)
                self.client = QdrantClient(path=self.storage_path)
                collections = [c.name for c in self.client.get_collections().collections]
                if self.COLLECTION_NAME not in collections:
                    self.client.create_collection(
                        collection_name=self.COLLECTION_NAME,
                        vectors_config=VectorParams(size=self.vector_dim, distance=Distance.COSINE),
                    )
            except Exception:
                self.client = None

    async def upsert_record(self, record: BeliefRecord):
        if not record.embedding or len(record.embedding) != self.vector_dim:
            record.embedding = LocalEdgeEmbeddingEngine.embed_text(f"{record.belief_key} {record.statement}")

        self._fallback_records[record.id] = record

        if self.client:
            try:
                point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, record.id))
                self.client.upsert(
                    collection_name=self.COLLECTION_NAME,
                    points=[
                        PointStruct(
                            id=point_id,
                            vector=record.embedding,
                            payload={
                                "record_id": record.id,
                                "belief_key": record.belief_key,
                                "statement": record.statement,
                                "outcome_status": record.outcome_status.value,
                                "confidence_score": record.confidence_score,
                                "criticality": record.criticality,
                                "is_private": record.is_private,
                                "is_tombstone": record.is_tombstone,
                            }
                        )
                    ]
                )
            except Exception:
                pass

    async def delete_record(self, record_id: str):
        self._fallback_records.pop(record_id, None)
        if self.client:
            try:
                point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, record_id))
                self.client.delete(
                    collection_name=self.COLLECTION_NAME,
                    points_selector=[point_id]
                )
            except Exception:
                pass

    async def hybrid_search(self, query_text: str, limit: int = 5) -> List[Tuple[BeliefRecord, float]]:
        """Executes offline hybrid search combining dense semantic cosine with lexical match boosts."""
        query_vec = LocalEdgeEmbeddingEngine.embed_text(query_text)
        tokens = set(query_text.lower().strip().split())
        scored: List[Tuple[BeliefRecord, float]] = []

        # Local Qdrant native vector search when available
        if self.client:
            try:
                search_res = self.client.search(
                    collection_name=self.COLLECTION_NAME,
                    query_vector=query_vec,
                    limit=limit * 2
                )
                for hit in search_res:
                    r_id = hit.payload.get("record_id")
                    if r_id in self._fallback_records:
                        rec = self._fallback_records[r_id]
                        if not rec.is_tombstone:
                            text_tokens = set(f"{rec.belief_key} {rec.statement}".lower().split())
                            overlap = len(tokens.intersection(text_tokens))
                            lexical_boost = 0.25 * (overlap / max(1, len(tokens)))
                            total_score = float(0.75 * hit.score + lexical_boost)
                            scored.append((rec, total_score))
                if scored:
                    scored.sort(key=lambda x: x[1], reverse=True)
                    return scored[:limit]
            except Exception:
                pass

        # In-memory deterministic fallback
        for record in self._fallback_records.values():
            if record.is_tombstone:
                continue
            sim = LocalEdgeEmbeddingEngine.cosine_similarity(query_vec, record.embedding)
            text_tokens = set(f"{record.belief_key} {record.statement}".lower().split())
            overlap = len(tokens.intersection(text_tokens))
            lexical_boost = 0.25 * (overlap / max(1, len(tokens)))
            total_score = float(0.75 * sim + lexical_boost)
            scored.append((record, total_score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:limit]

    def get_stats(self) -> Dict[str, Any]:
        """Returns collection storage metrics."""
        points_count = len(self._fallback_records)
        if self.client:
            try:
                info = self.client.get_collection(self.COLLECTION_NAME)
                points_count = info.points_count
            except Exception:
                pass
        return {
            "node_id": self.node_id,
            "engine": "Qdrant Edge (Embedded On-Disk)",
            "storage_path": self.storage_path,
            "dimension": self.vector_dim,
            "points_count": points_count
        }