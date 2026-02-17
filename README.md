# Linux Log Diagnosis RAG System

## Overview
This project implements a helper RAG (Retrieval-Augmented Generation) system for diagnosing Linux system errors. It is designed to take raw error logs as input, retrieve relevant troubleshooting information from a curated knowledge base, and generate structured root cause analysis and remediation steps.

## Architecture
1.  **Ingestion**: Collects data from various sources (text files, potential web scraping).
    - `src/ingest.py`: Handles data loading.
2.  **Processing**: Cleans and normalizes text data.
    - `src/clean.py`: Preprocessing logic.
    - `src/chunk.py`: Text chunking strategy.
3.  **Embedding & Storage**: Converts text chunks into vector embeddings and stores them in a vector database.
    - `src/embed.py`: Embedding generation.
    - `src/vector_store.py`: Interface to Vector DB (Chroma/FAISS).
4.  **Retrieval & Generation**: Given a user query (log), retrieves relevant context and generates a response.
    - `src/rag.py`: Main RAG pipeline.

## Setup
1.  **Clone Repository**
2.  **Install Dependencies**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Run Ingestion** (Example)
    ```bash
    python src/ingest.py --source data/raw
    ```
