import os
import sys

# Ensure src is in path if running directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from src.retrieve import Retriever
    from src.preprocess import QueryPreprocessor
    from src.llm_client import LLMClient
except ImportError:
    from retrieve import Retriever
    from preprocess import QueryPreprocessor
    from llm_client import LLMClient

class RAGSystem:
    def __init__(self, collection_name: str = "linux_rag_bge_m3", persist_directory: str = "data/vectordb", 
                 llm_provider: str = "lmstudio", llm_model: str = None, api_key: str = None):
        print("Initializing RAG System...")
        self.retriever = Retriever(collection_name=collection_name, persist_directory=persist_directory)
        self.preprocessor = QueryPreprocessor()
        self.llm_client = LLMClient(provider=llm_provider, model_name=llm_model, api_key=api_key)
        print(f"RAG System initialized with {llm_provider}/{llm_model}")
    
    def generate_response(self, user_input: str) -> str:
        """
        Generates a response using RAG.
        """
        # 1. Preprocess Query
        enhanced_query = self.preprocessor.enhance_query(user_input)
        print(f"Original Query: {user_input}")
        if enhanced_query != user_input:
            print(f"Enhanced Query: {enhanced_query}")

        # 2. Retrieve
        print(f"Retrieving context...")
        docs = self.retriever.retrieve(enhanced_query, k=3)
        
        context_text = "\n\n".join([f"--- Source: {d['metadata'].get('source', 'Unknown')} ---\n{d['content']}" for d in docs])
        
        if not context_text:
            context_text = "No specific knowledge found in the database. Relying on general knowledge."

        # 3. Construct Prompt
        prompt = f"""You are a Linux systems troubleshooting expert.

User Log/Query:
{user_input}

Relevant Knowledge:
{context_text}

Provide response in structured format:
Root Cause: [Diagnose the issue based on the log and context]
Explanation: [Explain why this is happening]
Suggested Fix: [Step-by-step resolution commands]
Commands: [List of commands to run]
Prevention: [How to avoid this in the future]
"""

        # 4. Generate
        print("Generating response via LLM...")
        response = self.llm_client.generate(prompt)
        
        return response, prompt

if __name__ == "__main__":
    rag = RAGSystem()
    response, prompt = rag.generate_response("How do I fix a segmentation fault in my C program?")
    print("\n--- RESPONSE ---\n")
    print(response)
