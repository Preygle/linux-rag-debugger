import os
import sys
import json
from flask import Flask, request, jsonify, render_template, Response, stream_with_context
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from src.rag import RAGSystem

app = Flask(__name__)

print("Initializing RAG System for web server...")
rag = RAGSystem()
print("RAG System ready.")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/chat/stream", methods=["POST"])
def chat_stream():
    """
    SSE streaming endpoint. Yields text chunks as they arrive from LM Studio,
    then sends a final 'done' event with sources.
    """
    data = request.get_json()
    if not data or not data.get("message", "").strip():
        return jsonify({"error": "Missing or empty 'message'"}), 400

    user_message = data["message"].strip()

    def generate():
        # ---- 1. Retrieve context (fast, non-streaming) ----
        try:
            enhanced_query = rag.preprocessor.enhance_query(user_message)
            docs = rag.retriever.retrieve(enhanced_query, k=3)
            context_text = "\n\n".join(
                [f"--- Source: {d['metadata'].get('source', 'Unknown')} ---\n{d['content']}"
                 for d in docs]
            )
            sources = list({
                os.path.basename(d["metadata"].get("source", ""))
                for d in docs if d["metadata"].get("source")
            })
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return

        if not context_text:
            context_text = "No specific knowledge found in the database. Relying on general knowledge."

        # ---- 2. Build prompt ----
        prompt = f"""You are a Linux systems troubleshooting expert.

User Log/Query:
{user_message}

Relevant Knowledge:
{context_text}

Provide response in structured format:
Root Cause: [Diagnose the issue based on the log and context]
Explanation: [Explain why this is happening]
Suggested Fix: [Step-by-step resolution commands]
Commands: [List of commands to run]
Prevention: [How to avoid this in the future]
"""

        # ---- 3. Stream LLM response chunk by chunk ----
        try:
            for chunk in rag.llm_client.stream_generate(prompt):
                yield f"data: {json.dumps({'chunk': chunk})}\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            return

        # ---- 4. Final event with sources ----
        yield f"data: {json.dumps({'done': True, 'sources': sources})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# Legacy non-streaming endpoint (kept for compatibility)
@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json()
    if not data or "message" not in data:
        return jsonify({"error": "Missing 'message' field"}), 400
    user_message = data["message"].strip()
    if not user_message:
        return jsonify({"error": "Empty message"}), 400
    try:
        response, prompt = rag.generate_response(user_message)
        docs = rag.retriever.retrieve(user_message, k=3)
        sources = list({
            os.path.basename(d["metadata"].get("source", ""))
            for d in docs if d["metadata"].get("source")
        })
        return jsonify({"response": response, "sources": sources})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=5000, threaded=True)
