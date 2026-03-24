import os
import requests
from typing import List

# LM Studio runs a local OpenAI-compatible server.
# Default base URL can be overridden via .env: LM_STUDIO_BASE_URL
LM_STUDIO_BASE_URL = os.getenv("LM_STUDIO_BASE_URL", "http://localhost:1234/v1")
LM_STUDIO_EMBED_MODEL = os.getenv("LM_STUDIO_EMBED_MODEL", "ggml-org/bge-m3-Q8_0-GGUF")


class Embedder:
    def __init__(
        self,
        model_name: str = LM_STUDIO_EMBED_MODEL,
        base_url: str = LM_STUDIO_BASE_URL,
    ):
        """
        Embedder using LM Studio's local OpenAI-compatible embedding endpoint.
        Make sure LM Studio is running and the model is loaded before calling this.
        """
        self.model_name = model_name
        self.endpoint = f"{base_url.rstrip('/')}/embeddings"
        print(f"[Embedder] Using LM Studio at {self.endpoint} with model: {self.model_name}")

    def _batch_embed(self, texts: List[str]) -> List[List[float]]:
        """Send a batch of texts to LM Studio and return their embeddings."""
        payload = {
            "model": self.model_name,
            "input": texts,
        }
        try:
            resp = requests.post(self.endpoint, json=payload, timeout=300)
            resp.raise_for_status()
            data = resp.json()
            # LM Studio returns { "data": [{ "embedding": [...] }, ...] }
            return [item["embedding"] for item in data["data"]]
        except requests.exceptions.ConnectionError:
            raise RuntimeError(
                "[Embedder] Cannot connect to LM Studio. "
                "Make sure LM Studio is open and the embedding model is loaded."
            )
        except Exception as e:
            raise RuntimeError(f"[Embedder] Embedding request failed: {e}")

    def embed_documents(self, documents: List[str], batch_size: int = 20) -> List[List[float]]:
        """
        Generate embeddings for a list of documents, batched for efficiency.
        """
        if not documents:
            return []

        all_embeddings: List[List[float]] = []
        total = len(documents)

        for i in range(0, total, batch_size):
            batch = documents[i : i + batch_size]
            embeddings = self._batch_embed(batch)
            all_embeddings.extend(embeddings)
            print(f"  [Embedder] Embedded {min(i + batch_size, total)}/{total} documents...")

        return all_embeddings

    def embed_query(self, query: str) -> List[float]:
        """
        Generate embedding for a single query string.
        """
        return self._batch_embed([query])[0]


if __name__ == "__main__":
    embedder = Embedder()
    test_query = "kernel panic: not syncing: VFS: Unable to mount root fs"
    emb = embedder.embed_query(test_query)
    print(f"Embedding dimension: {len(emb)}")
    print(f"First 5 values: {emb[:5]}")
