"""
Zero-cost, fully offline semantic embedding engine.
Requires no external paid APIs or cloud keys.
"""
import hashlib
import numpy as np
from typing import List

try:
    from fastembed import TextEmbedding
    HAS_FASTEMBED = True
    _FASTEMBED_MODEL = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
except Exception:
    HAS_FASTEMBED = False
    _FASTEMBED_MODEL = None


class LocalEdgeEmbeddingEngine:
    DIMENSION = 384 if HAS_FASTEMBED else 64

    @classmethod
    def embed_text(cls, text: str) -> List[float]:
        """Generates embeddings locally on device with zero API cost."""
        if HAS_FASTEMBED and _FASTEMBED_MODEL is not None:
            try:
                embeddings = list(_FASTEMBED_MODEL.embed([text]))
                return embeddings[0].tolist()
            except Exception:
                pass

        # Standalone local deterministic projection fallback
        tokens = text.lower().strip().split()
        if not tokens:
            return [0.0] * cls.DIMENSION
        
        vec = np.zeros(cls.DIMENSION, dtype=np.float32)
        for token in tokens:
            padded = f"_{token}_"
            for i in range(len(padded) - 2):
                ngram = padded[i:i+3]
                h = int(hashlib.sha256(ngram.encode('utf-8')).hexdigest(), 16)
                idx = h % cls.DIMENSION
                val = 1.0 if ((h >> 8) % 2 == 0) else -1.0
                vec[idx] += val

        norm = np.linalg.norm(vec)
        if norm > 1e-6:
            vec = vec / norm
        return vec.tolist()

    @staticmethod
    def cosine_similarity(v1: List[float], v2: List[float]) -> float:
        a = np.array(v1, dtype=np.float32)
        b = np.array(v2, dtype=np.float32)
        norm_a = np.linalg.norm(a)
        norm_b = np.linalg.norm(b)
        if norm_a < 1e-6 or norm_b < 1e-6:
            return 0.0
        return float(np.dot(a, b) / (norm_a * norm_b))