import argparse
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

try:
    from src.ingest import load_documents
    from src.clean import clean_text
    from src.chunk import chunk_text
    from src.embed import Embedder
    from src.vector_store import VectorStore
    from src.rag import RAGSystem
except ImportError:
    # If installed as package
    from ingest import load_documents
    from clean import clean_text
    from chunk import chunk_text
    from embed import Embedder
    from vector_store import VectorStore
    from rag import RAGSystem

def ingest_data(source_path, source_type, limit, output_file=None):
    print(f"Ingesting data from {source_type} (limit: {limit})...")
    if source_type == 'local' and source_path:
        print(f"Source path: {source_path}")
    
    # 1. Load
    raw_docs = load_documents(source_path, source_type, limit)
    print(f"Loaded {len(raw_docs)} documents.")

    # 1.5 Export Raw Data (for training)
    if output_file:
        import json
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for doc in raw_docs:
                    json.dump(doc, f)
                    f.write('\n')
            print(f"Exported {len(raw_docs)} documents to {output_file}")
        except Exception as e:
            print(f"Error exporting to file: {e}")
    
    # 2. Process
    processed_docs = []
    metadatas = []
    
    for doc in raw_docs:
        cleaned = clean_text(doc['content'])
        chunks = chunk_text(cleaned)
        for chunk in chunks:
            processed_docs.append(chunk)
            metadatas.append(doc['metadata'])
            
    print(f"Created {len(processed_docs)} chunks.")
    
    # 3. Embed and Store
    if processed_docs:
        print("Embedding...")
        embedder = Embedder()
        embeddings = embedder.embed_documents(processed_docs)
        
        print("Storing in vector database...")
        vs = VectorStore()
        vs.add_documents(processed_docs, metadatas, embeddings=embeddings)
        print("Ingestion complete.")
    else:
        print("No data to ingest.")

def query_system(text, provider, model):
    print(f"Querying: {text}")
    print(f"Using LLM: {provider}/{model}")
    try:
        rag = RAGSystem(llm_provider=provider, llm_model=model)
        response, prompt = rag.generate_response(text)
        print("\n=== RESPONSE ===\n")
        print(response)
    except Exception as e:
        print(f"Error during query: {e}")

def main():
    parser = argparse.ArgumentParser(description="Linux Log Diagnosis RAG System")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Ingest command
    ingest_parser = subparsers.add_parser("ingest", help="Ingest data")
    ingest_parser.add_argument("--source", help="Path to source directory (for local)", default=None)
    ingest_parser.add_argument("--type", choices=['local', 'stackexchange', 'github', 'web'], default='local', help="Source type")
    ingest_parser.add_argument("--limit", type=int, default=10, help="Max documents to fetch")
    ingest_parser.add_argument("--output", help="Path to export JSONL file (for training data)", default=None)
    
    # Query command
    query_parser = subparsers.add_parser("query", help="Query the RAG system")
    query_parser.add_argument("text", help="Query text or log")
    query_parser.add_argument("--provider", choices=['ollama', 'openai'], default='ollama', help="LLM Provider")
    query_parser.add_argument("--model", default='llama2', help="LLM Model Name (e.g., llama2, gpt-3.5-turbo)")
    
    args = parser.parse_args()
    
    if args.command == "ingest":
        if args.type == 'local' and not args.source:
             print("Error: --source is required for type 'local'")
        else:
             ingest_data(args.source, args.type, args.limit, args.output)
    elif args.command == "query":
        query_system(args.text, args.provider, args.model)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
