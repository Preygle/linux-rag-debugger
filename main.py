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

    # 1.5 Export Raw Data (for training) — extract only, no embedding
    if output_file:
        _export_to_jsonl(raw_docs, output_file)
        return
    
    # 2. Process (only when NOT exporting)
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

def _export_to_jsonl(raw_docs, output_file):
    """Export documents to JSONL (appends, does not overwrite)."""
    import json
    try:
        with open(output_file, 'a', encoding='utf-8') as f:
            for doc in raw_docs:
                meta = doc.get('metadata', {})
                if 'question' in meta and 'answer' in meta:
                    training_example = {
                        "messages": [
                            {"role": "user", "content": meta['question']},
                            {"role": "assistant", "content": meta['answer']}
                        ],
                        "source": "stackexchange"
                    }
                else:
                    training_example = {
                        "text": doc.get('content'),
                        "source": meta.get('type', 'unknown')
                    }
                json.dump(training_example, f)
                f.write('\n')
        print(f"Exported {len(raw_docs)} documents to {output_file}")
    except Exception as e:
        print(f"Error exporting to file: {e}")

def extract_all(output_dir):
    """
    Extract data from ALL sources at 95% of daily rate limits.
    Persistent deduplication ensures no repeated data across runs.
    """
    # 95% of max daily limits
    LIMITS = {
        'stackexchange': 9500,
        'github': 950,
        'web': 950,
    }

    os.makedirs(output_dir, exist_ok=True)

    for source_type, limit in LIMITS.items():
        output_file = os.path.join(output_dir, f"{source_type}.jsonl")
        print(f"\n{'='*50}")
        print(f"Extracting: {source_type} (limit: {limit})")
        print(f"Output: {output_file}")
        print(f"{'='*50}")
        ingest_data(None, source_type, limit, output_file)

    print(f"\n{'='*50}")
    print("All extractions complete!")
    print(f"Files saved to: {output_dir}/")
    print(f"{'='*50}")

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
    
    # Ingest command (single source)
    ingest_parser = subparsers.add_parser("ingest", help="Ingest data from a single source")
    ingest_parser.add_argument("--source", help="Path to source directory (for local)", default=None)
    ingest_parser.add_argument("--type", choices=['local', 'stackexchange', 'github', 'web', 'jsonl'], default='local', help="Source type")
    ingest_parser.add_argument("--limit", type=int, default=10, help="Max documents to fetch")
    ingest_parser.add_argument("--output", help="Path to export JSONL file (extract only, no embedding)", default=None)
    
    # Extract-all command (all sources, 95% rate limits)
    extract_parser = subparsers.add_parser("extract-all", help="Extract data from ALL sources (95%% of rate limits)")
    extract_parser.add_argument("--output-dir", default="data/training", help="Directory to save JSONL files")
    
    # Query command
    query_parser = subparsers.add_parser("query", help="Query the RAG system")
    query_parser.add_argument("text", help="Query text or log")
    query_parser.add_argument("--provider", choices=['ollama', 'openai', 'lmstudio'], default='lmstudio', help="LLM Provider")
    query_parser.add_argument("--model", default=None, help="LLM Model Name (overrides .env default)")
    
    args = parser.parse_args()
    
    if args.command == "ingest":
        if args.type == 'local' and not args.source:
             print("Error: --source is required for type 'local'")
        else:
             ingest_data(args.source, args.type, args.limit, args.output)
    elif args.command == "extract-all":
        extract_all(args.output_dir)
    elif args.command == "query":
        query_system(args.text, args.provider, args.model)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
