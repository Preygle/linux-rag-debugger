import chromadb
from typing import List, Dict, Optional, Any
import os
import uuid

class VectorStore:
    def __init__(self, collection_name: str = "linux_rag_bge_m3", persist_directory: str = "data/vectordb"):
        """
        Initializes the ChromaDB client and collection.
        """
        # Ensure directory exists
        if not os.path.exists(persist_directory):
            try:
                os.makedirs(persist_directory)
            except OSError as e:
                print(f"Error creating directory {persist_directory}: {e}")

        self.client = chromadb.PersistentClient(path=persist_directory)
        self.collection = self.client.get_or_create_collection(name=collection_name)
        # print(f"Vector store initialized at {persist_directory}, collection: {collection_name}")

    def add_documents(self, documents: List[str], metadatas: Optional[List[Dict[str, Any]]] = None, ids: Optional[List[str]] = None, embeddings: Optional[List[List[float]]] = None, batch_size: int = 1000):
        """
        Adds documents to the vector store in batches to avoid ChromaDB's 5461-item hard limit.
        """
        if not documents:
            return

        count = len(documents)
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in range(count)]
        if metadatas is None:
            metadatas = [{} for _ in range(count)]

        for i in range(0, count, batch_size):
            batch_end = min(i + batch_size, count)
            self.collection.add(
                documents=documents[i:batch_end],
                metadatas=metadatas[i:batch_end],
                ids=ids[i:batch_end],
                embeddings=embeddings[i:batch_end] if embeddings else None,
            )
            print(f"  [VectorStore] Stored {batch_end}/{count} chunks...")

    def query(self, query_embeddings: List[List[float]], n_results: int = 5) -> Dict:
        """
        Queries the vector store using embeddings.
        """
        return self.collection.query(
            query_embeddings=query_embeddings,
            n_results=n_results
        )
    
    def count(self) -> int:
        return self.collection.count()

if __name__ == "__main__":
    vs = VectorStore()
    print(f"Collection count: {vs.count()}")
