from typing import List, Dict, Any, Optional
import numpy as np
from concord.models import BeliefRecord
from concord.embeddings import LocalEdgeEmbeddingEngine

try:
    from qdrant_client import QdrantClient
    from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue
    HAS_QDRANT = True
except ImportError:
    HAS_QDRANT = False

class EdgeQdrantVectorStore:
    """
    Edge vector storage engine interfacing directly with Qdrant (in-memory or embedded)
    or fallback local vector store. Implements dense semantic + metadata hybrid search.
    """
    COLLECTION_NAME = "concord_beliefs"
    VECTOR_SIZE = LocalEdgeEmbeddingEngine.DIMENSION

    def __init__(self, node_id: str):
        self.node_id = node_id
        self.client = None
        self._fallback_records: Dict[str, BeliefRecord] = {}

        if HAS_QDRANT:
            try:
                self.client = QdrantClient(location=":memory:")
                self.client.create_collection(
                    collection_name=self.COLLECTION_NAME,
                    vectors_config=VectorParams(size=self.VECTOR_SIZE, distance=Distance.COSINE),
                )
            except Exception:
                self.client = None

    async def upsert_record(self, record: BeliefRecord):
        if not record.embedding or len(record.embedding) != self.VECTOR_SIZE:
            record.embedding = LocalEdgeEmbeddingEngine.embed_text(f"{record.belief_key} {record.statement}")

        self._fallback_records[record.id] = record

        if self.client:
            try:
                import uuid
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

    async def hybrid_search(self, query_text: str, limit: int = 5) -> List[tuple[BeliefRecord, float]]:
        """
        Performs hybrid retrieval: Combines dense cosine similarity with keyword exact-match boosts.
        Returns list of (BeliefRecord, score).
        """
        query_vec = LocalEdgeEmbeddingEngine.embed_text(query_text)
        query_lower = query_text.lower().strip()
        tokens = set(query_lower.split())

        scored_records: List[tuple[BeliefRecord, float]] = []

        for record in self._fallback_records.values():
            if record.is_tombstone:
                continue

            # Dense Semantic Similarity
            dense_sim = LocalEdgeEmbeddingEngine.cosine_similarity(query_vec, record.embedding)
            
            # Lexical / Keyword Overlap Boost
            text_tokens = set(f"{record.belief_key} {record.statement}".lower().split())
            overlap = len(tokens.intersection(text_tokens))
            lexical_boost = 0.3 * (overlap / max(1, len(tokens)))

            # Hybrid Combined Score
            total_score = float(0.7 * dense_sim + lexical_boost)
            scored_records.append((record, total_score))

        scored_records.sort(key=lambda x: x[1], reverse=True)
        return scored_records[:limit]