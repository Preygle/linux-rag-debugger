from typing import List

def chunk_text(text: str, chunk_size: int = 500, chunk_overlap: int = 50) -> List[str]:
    """
    Splits text into chunks of specified size with overlap.
    """
    chunks = []
    if not text:
        return []
    
    if len(text) <= chunk_size:
        return [text]

    start = 0
    text_len = len(text)
    
    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunk = text[start:end]
        chunks.append(chunk)
        
        if end == text_len:
            break
            
        start += chunk_size - chunk_overlap
        
    return chunks

if __name__ == "__main__":
    text = "A" * 1200
    chunks = chunk_text(text, 500, 50)
    print(f"Text length: {len(text)}")
    print(f"Number of chunks: {len(chunks)}")
    for i, c in enumerate(chunks):
        print(f"Chunk {i}: length {len(c)}")
