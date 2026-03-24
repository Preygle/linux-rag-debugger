from typing import List, Dict, Any
try:
    from src.embed import Embedder
    from src.vector_store import VectorStore
except ImportError:
    # Fallback for running script directly inside src/
    from embed import Embedder
    from vector_store import VectorStore

class Retriever:
    def __init__(self, collection_name: str = "linux_rag_bge_m3", persist_directory: str = "data/vectordb", embedding_model: str = None):
        """
        Initializes the retriever with an embedder and vector store.
        """
        # embedding_model is ignored — Embedder now reads from .env / LM Studio defaults
        self.embedder = Embedder()
        self.vector_store = VectorStore(collection_name=collection_name, persist_directory=persist_directory)

    def retrieve(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """
        Retrieves top-k relevant documents for a query.
        """
        # Embed the query
        query_emb = self.embedder.embed_query(query)
        
        # Query the vector store
        results = self.vector_store.query([query_emb], n_results=k)
        
        # Parse results from ChromaDB format
        documents = []
        if results and results.get('documents'):
            docs = results['documents'][0]
            metas = results['metadatas'][0] if results['metadatas'] else [{}] * len(docs)
            ids = results['ids'][0] if results['ids'] else [""] * len(docs)
            distances = results['distances'][0] if results.get('distances') else [0.0] * len(docs)

            for i in range(len(docs)):
                doc = {
                    'content': docs[i],
                    'metadata': metas[i],
                    'id': ids[i],
                    'score': distances[i] if distances else None
                }
                documents.append(doc)
        
        return documents

if __name__ == "__main__":
    # Test
    retriever = Retriever()
    if retriever.vector_store.count() == 0:
        print("Vector store is empty, adding mock data...")
        mock_text = "To fix 'error while loading shared libraries', check if the library is installed and in LD_LIBRARY_PATH."
        retriever.vector_store.add_documents([mock_text], [{"source": "mock"}])
        
    results = retriever.retrieve("shared library error")
    for res in results:
        print(f"Content: {res['content']}\nScore: {res['score']}\n")
