import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.retrieve import Retriever
except ImportError:
    # If running directly in src
    try:
        from retrieve import Retriever
    except ImportError:
        print("Could not import Retriever. Ensure you run this from project root or src.")
        sys.exit(1)

def run_evaluation():
    try:
        retriever = Retriever()
    except Exception as e:
        print(f"Failed to initialize retriever: {e}")
        return

    try:
        count = retriever.vector_store.count()
        if count == 0:
            print("Vector store is empty. Please run ingestion first.")
            print("Run: python main.py ingest --source data/raw")
            return
    except Exception as e:
        print(f"Error checking vector store: {e}")
        return

    # Test cases: (Query, Expected Keywords)
    test_cases = [
        ("Kernel panic VFS unable to mount root fs", ["initramfs", "grub", "root partition"]),
        ("error while loading shared libraries: libssl.so", ["libssl", "LD_LIBRARY_PATH", "install"]),
        ("Segmentation fault (core dumped)", ["gdb", "buffer overflow", "stack overflow"])
    ]
    
    print("\nStarting Evaluation...")
    print(f"Total Test Cases: {len(test_cases)}\n")
    
    passed_count = 0
    
    for query, keywords in test_cases:
        print(f"Query: '{query}'")
        try:
            results = retriever.retrieve(query, k=3)
            found_any = False
            relevant_doc = ""
            
            for res in results:
                content = res['content'].lower()
                # Check if ANY of the expected keywords are present
                for kw in keywords:
                    if kw.lower() in content:
                        found_any = True
                        relevant_doc = res['content'][:100].replace('\n', ' ') + "..."
                        break
                if found_any:
                    break
            
            if found_any:
                print(f"  [PASS] Found relevant context: {relevant_doc}")
                passed_count += 1
            else:
                print(f"  [FAIL] Did not find any of: {keywords}")
                
        except Exception as e:
            print(f"  [ERROR] Processing query failed: {e}")
            
        print("-" * 40)

    print(f"\nEvaluation Complete.")
    print(f"Accuracy: {passed_count}/{len(test_cases)} ({passed_count/len(test_cases):.1%})")

if __name__ == "__main__":
    run_evaluation()
