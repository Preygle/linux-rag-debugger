import os
import json
import time
import asyncio
import aiohttp
import argparse
import re
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

class AsyncGroqFormatter:
    def __init__(self, rpm_limit=30):
        self.api_key = os.getenv("groq") # Using the 'groq' key from .env as requested
        if not self.api_key:
            raise ValueError("'groq' environment variable is not set correctly in .env")
        
        self.model_name = 'llama-3.3-70b-versatile'
        self.api_url = "https://api.groq.com/openai/v1/chat/completions"
        
        # Concurrency limit based on RPM
        self.semaphore = asyncio.Semaphore(rpm_limit)
        self.max_chunk_chars = 8000 # Map phase chunk size (drastically lowered to avoid massive TPM Groq spikes)

    def _chunk_text_structure_aware(self, text: str) -> List[str]:
        """
        Splits massive logs without breaking markdown code blocks or stack traces.
        """
        chunks = []
        current_chunk = []
        current_length = 0
        in_code_block = False

        for line in text.split('\n'):
            if line.startswith('```'):
                in_code_block = not in_code_block

            line_len = len(line) + 1
            
            # If we hit the limit, AND we are not inside a code block, flush the chunk
            if current_length + line_len > self.max_chunk_chars and not in_code_block:
                if current_chunk:
                    chunks.append("\n".join(current_chunk))
                    current_chunk = []
                    current_length = 0
            
            current_chunk.append(line)
            current_length += line_len

        # Flush remainder
        if current_chunk:
            chunks.append("\n".join(current_chunk))

        return chunks

    def _extract_json(self, text: str) -> str:
        """Extracts the outermost JSON object or array from a string."""
        if not text:
            return ""
            
        text = text.strip()
        start_obj = text.find('{')
        end_obj = text.rfind('}')
        start_arr = text.find('[')
        end_arr = text.rfind(']')
        
        if start_obj != -1 and end_obj != -1 and (start_arr == -1 or start_obj < start_arr):
            return text[start_obj:end_obj+1]
        elif start_arr != -1 and end_arr != -1:
            return text[start_arr:end_arr+1]
        return text

    async def _call_llm(self, system_prompt: str, user_prompt: str, attempt: int = 1) -> str:
        """Async wrapper for Groq API using OpenAI-compatible endpoint with backoff."""
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1,
            "response_format": {"type": "json_object"}
        }
        
        async with self.semaphore:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(self.api_url, headers=headers, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as response:
                        if response.status == 200:
                            data = await response.json()
                            if 'choices' in data and len(data['choices']) > 0:
                                return data['choices'][0]['message']['content']
                            return None
                        else:
                            error_text = await response.text()
                            if response.status in [400, 401, 403]:
                                print(f"FATAL API ERROR {response.status}: {error_text}")
                                raise ValueError(f"HTTP {response.status}: {error_text}")
                            raise RuntimeError(f"HTTP {response.status}: {error_text}")
                            
            except BaseException as e:
                # Catch rate limits or service errors
                if attempt <= 10 and not isinstance(e, KeyboardInterrupt) and not isinstance(e, ValueError):
                    wait_time = 8 ** attempt
                    
                    # If this is our manual RuntimeError 429 from Groq
                    if isinstance(e, RuntimeError) and "429" in str(e):
                        # Try to parse the explicit wait time
                        match = re.search(r"Please try again in (\d+\.?\d*)s", str(e))
                        if match:
                            wait_time = float(match.group(1)) + 1.0 # Buffer safely
                            print(f"    [~] Rate limit hit. Groq requested we wait {wait_time:.2f}s... (Attempt {attempt})")
                        else:
                            print(f"    [~] Rate limit hit, backing off via formula... (Attempt {attempt})")
                    else:
                        print(f"    [~] Network/API error, backing off... (Attempt {attempt}) - {type(e).__name__}")
                    
                    await asyncio.sleep(wait_time) 
                    return await self._call_llm(system_prompt, user_prompt, attempt + 1)
                
                if isinstance(e, KeyboardInterrupt) or isinstance(e, asyncio.CancelledError):
                    raise
                
                print(f"    [!] Failed LLM call finally: {e}")
                return None

    async def _map_chunk(self, chunk: str, chunk_index: int) -> Dict[str, Any]:
        """MAP PHASE: Extract problems and solutions from a single chunk."""
        system_prompt = "You are a strict, objective data-extraction parser. Your ONLY job is to extract technical troubleshooting data from the provided text chunk. Return ONLY valid JSON."
        user_prompt = f"""
CRITICAL RULES:
1. DO NOT use outside knowledge. 
2. DO NOT invent, infer, or guess a solution if one is not explicitly stated in the text.
3. If the text discusses an error but provides no solution, you must explicitly state: "No solution provided in this text chunk."
4. Preserve exact error codes, file paths, versions, and hardware specs exactly as written.

EXTRACT THE FOLLOWING INTO A JSON OBJECT:
{{
  "hardware_or_env": "(STRING: Any hardware, OS, or software versions mentioned)",
  "problem": "(STRING: A concise summary of the issue being experienced)",
  "raw_logs": "(STRING: Exact copy-paste of any error messages or terminal outputs. Use \\n for newlines. DO NOT USE AN ARRAY.)",
  "proposed_solution": "(STRING: What the users explicitly tried or suggested to fix it)"
}}

TEXT CHUNK TO PROCESS:
{chunk}

Reply ONLY with the valid JSON object. Do not include any other text or conversational preamble.
"""
        result = await self._call_llm(system_prompt, user_prompt)
        try:
            if result:
                clean_json = self._extract_json(result)
                parsed = json.loads(clean_json)
                parsed['chunk_index'] = chunk_index
                return parsed
        except json.JSONDecodeError:
            print(f"    [x] Chunk {chunk_index} failed strict JSON mapping")
        return None

    async def _reduce_chunks(self, mapped_results: List[Dict[str, Any]], original_doc_id: str) -> Dict[str, Any]:
        """REDUCE PHASE: Synthesize all mapped chunk extractions into a final structured output."""
        valid_results = [r for r in mapped_results if r]
        
        if not valid_results:
            return None # No useful diagnostic info found across any chunks

        system_prompt = "You are a strict data-merging parser. You are receiving an array of JSON objects representing extracted parts of a single Linux troubleshooting thread. Return ONLY valid JSON."
        
        compiled_data = json.dumps(valid_results, indent=2)
        user_prompt = f"""
CRITICAL RULES:
1. Synthesize this array into ONE final, cohesive JSON object.
2. DO NOT add any outside knowledge. Base your final output strictly on the provided JSON objects.
3. If multiple solutions were proposed across the chunks, list the one that the users confirmed worked. If unconfirmed, list all proposed steps.
4. Output MUST be valid JSON, ready to be appended to a JSONL file.

REQUIRED OUTPUT SCHEMA:
{{
  "doc_id": "{original_doc_id}",
  "domain": "(STRING: e.g., 'systemd', 'networking', 'kernel')",
  "hardware_env": "(STRING: Merged string of relevant specs/versions)",
  "problem": "(STRING: Cohesive summary of the entire issue)",
  "raw_logs": "(STRING: Combined critical error strings. Use \\n for newlines. DO NOT USE AN ARRAY.)",
  "solution": "(STRING: The explicitly stated fix, or 'Unresolved')"
}}

ARRAY OF EXTRACTED CHUNKS:
{compiled_data}

Reply ONLY with the valid JSON object. Do not include any other text or conversational preamble.
"""
        
        result = await self._call_llm(system_prompt, user_prompt)
        try:
            if result:
                clean_json = self._extract_json(result)
                return json.loads(clean_json)
        except json.JSONDecodeError:
            pass # Silent fail here, the process_document loop will catch the None return
        return None

    async def process_document(self, raw_text: str, source_id: str) -> Dict[str, Any]:
        """Main pipeline: Split -> Map -> Reduce"""
        chunks = self._chunk_text_structure_aware(raw_text)
        
        if len(chunks) == 0:
            return None
        
        # Initialize task
        if len(chunks) > 1:
            print(f"    [~] Mapping {len(chunks)} chunk(s)...", end="\r")
        
        # MAP concurrently
        tasks = [self._map_chunk(chunk, i) for i, chunk in enumerate(chunks)]
        mapped_results = await asyncio.gather(*tasks)

        # REDUCE
        final_json = await self._reduce_chunks(mapped_results, source_id)
        return final_json

    async def process_file(self, raw_file_path: str, output_dir: str):
        if not os.path.exists(raw_file_path):
            print(f"File not found: {raw_file_path}")
            return

        source_name = os.path.basename(raw_file_path).replace('.jsonl', '')
        processed_file_path = os.path.join(output_dir, f"processed_{source_name}.jsonl")
        temp_raw_path = raw_file_path + ".temp"

        print(f"\nProcessing {raw_file_path} -> {processed_file_path}")
        
        # Fast file loading
        with open(raw_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        processed_count = 0
        skipped_count = 0

        last_processed_idx = -1
        try:
            # We open the destination in append mode to incrementally save
            with open(processed_file_path, 'a', encoding='utf-8') as out_f, \
                 open(temp_raw_path, 'w', encoding='utf-8') as raw_out_f:
                
                for i, line in enumerate(lines):
                    last_processed_idx = i
                    if not line.strip(): continue
                    
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        raw_out_f.write(line)
                        continue
                    
                    # Idempotency check 
                    if data.get("formatting_done", False):
                        raw_out_f.write(json.dumps(data) + '\n')
                        skipped_count += 1
                        continue

                    # Extract raw text depending on schema
                    raw_text = ""
                    if 'messages' in data: 
                        raw_text = "\n".join([f"{m['role']}: {m['content']}" for m in data['messages']])
                    elif 'text' in data:   
                        raw_text = data['text']

                    if not raw_text:
                        raw_out_f.write(json.dumps(data) + '\n')
                        continue

                    # Execute Map-Reduce via Groq
                    source_id = data.get('source', 'unknown')
                    final_structured = await self.process_document(raw_text, source_id)
                    
                    if final_structured:
                        # Add metadata
                        final_structured['source_file'] = raw_file_path
                        final_structured['original_link'] = data.get('source', '')
                        
                        # Append to processed file
                        out_f.write(json.dumps(final_structured) + '\n')
                        out_f.flush() # Ensure it hits disk immediately
                        
                        # Mark raw file as done
                        data['formatting_done'] = True
                        processed_count += 1
                        print(f"  [✓] Processed: {data.get('source', 'unknown')} | Total: {processed_count}")
                    else:
                        print(f"  [x] Failed extraction: {data.get('source', 'unknown')}")
                    
                    # Save the raw row (either flagged true, or false if it failed so we retry later)
                    raw_out_f.write(json.dumps(data) + '\n')
        except BaseException as e:
            if isinstance(e, KeyboardInterrupt) or isinstance(e, asyncio.CancelledError):
                print("\n[!] Script interrupted. Gracefully saving progress...")
            else:
                print(f"\n[!] Unexpected Error. Gracefully saving progress... {e}")
            raise
        finally:
            # Safely write all remaining unprocessed lines to the temp file so they aren't lost
            with open(temp_raw_path, 'a', encoding='utf-8') as raw_out_f:
                for remaining_line in lines[last_processed_idx + 1:]:
                    if remaining_line.strip():
                        raw_out_f.write(remaining_line)
                        if not remaining_line.endswith('\n'):
                            raw_out_f.write('\n')
            
            # Atomic replacement of the raw file to save the formatting_done flags
            os.replace(temp_raw_path, raw_file_path)

        print(f"Finished {source_name} | Mapped/Reduced: {processed_count} | Skipped: {skipped_count}")

async def main():
    parser = argparse.ArgumentParser(description="Async Map-Reduce JSONL Formatter for Groq Llama 3.")
    parser.add_argument("--directory", default=os.path.join("data", "training"), help="Input directory")
    parser.add_argument("--output", default=os.path.join("data", "processed"), help="Output directory")
    parser.add_argument("--rpm", type=int, default=3, help="Requests per minute (concurrency limit)")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    formatter = AsyncGroqFormatter(rpm_limit=args.rpm)
    
    if os.path.exists(args.directory):
        for file in os.listdir(args.directory):
            if file.endswith(".jsonl") and not file.startswith("processed_"):
                await formatter.process_file(os.path.join(args.directory, file), args.output)
    else:
        print(f"Directory {args.directory} does not exist.")

if __name__ == "__main__":
    # Windows asyncio fix for ProactorEventLoop
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    asyncio.run(main())
