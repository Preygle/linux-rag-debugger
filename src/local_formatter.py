import os
import json
import asyncio
import aiohttp
import argparse
import re
from typing import List, Dict, Any

class AsyncLocalFormatter:
    def __init__(self, base_url="http://localhost:1234/v1", max_concurrent_requests=5):
        self.base_url = base_url
        self.model_name = "bartowski/Meta-Llama-3.1-8B-Instruct-GGUF/Meta-Llama-3.1-8B-Instruct-Q6_K_L.gguf"
        # Concurrency limit so we don't overwhelm LM Studio's local queue
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.max_chunk_chars = 25000 # Map phase chunk size (increased for 16k+ context)

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
        """Extracts the outermost JSON object or array from a string containing possible conversational text."""
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

    async def _call_llm(self, session: aiohttp.ClientSession, system_prompt: str, user_prompt: str, attempt: int = 1) -> str:
        """Async wrapper for LM Studio HTTP API with backoff."""
        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 2000,
            "response_format": {"type": "json_object"}
        }

        async with self.semaphore:
            try:
                async with session.post(f"{self.base_url}/chat/completions", json=payload, timeout=aiohttp.ClientTimeout(total=900)) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data['choices'][0]['message']['content'].strip().replace("```json", "").replace("```", "")
                    else:
                        text = await response.text()
                        print(f"API Error {response.status}: {text}")
                        raise Exception(f"HTTP {response.status}")
            except Exception as e:
                if attempt <= 3:
                    await asyncio.sleep(2 ** attempt) # Exponential backoff
                    return await self._call_llm(session, system_prompt, user_prompt, attempt + 1)
                print(f"Failed LLM call after 3 attempts: {e}")
                return None

    async def _map_chunk(self, session: aiohttp.ClientSession, chunk: str, chunk_index: int) -> Dict[str, Any]:
        """MAP PHASE: Extract problems and solutions from a single chunk."""
        # Using a JSON schema enforcing prompt for structured extraction
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
        result = await self._call_llm(session, system_prompt, user_prompt)
        try:
            if result:
                clean_json = self._extract_json(result)
                parsed = json.loads(clean_json)
                parsed['chunk_index'] = chunk_index
                return parsed
        except json.JSONDecodeError:
            print(f"    [x] Chunk {chunk_index} failed strict JSON mapping")
        return None

    async def _reduce_chunks(self, session: aiohttp.ClientSession, mapped_results: List[Dict[str, Any]], original_doc_id: str) -> Dict[str, Any]:
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
        
        result = await self._call_llm(session, system_prompt, user_prompt)
        try:
            if result:
                clean_json = self._extract_json(result)
                return json.loads(clean_json)
        except json.JSONDecodeError:
            pass # Silent fail here, the process_document loop will catch the None return
        return None

    async def process_document(self, session: aiohttp.ClientSession, raw_text: str, source_id: str) -> Dict[str, Any]:
        """Main pipeline: Split -> Map -> Reduce"""
        chunks = self._chunk_text_structure_aware(raw_text)
        
        if len(chunks) == 0:
            return None
        
        # Initialize task
        if len(chunks) > 1:
            print(f"    [~] Mapping {len(chunks)} chunk(s)...", end="\r")
        # MAP concurrently
        tasks = [self._map_chunk(session, chunk, i) for i, chunk in enumerate(chunks)]
        mapped_results = await asyncio.gather(*tasks)

        # REDUCE
        final_json = await self._reduce_chunks(session, mapped_results, source_id)
        return final_json

    async def process_file(self, raw_file_path: str, output_dir: str):
        if not os.path.exists(raw_file_path):
            print(f"File not found: {raw_file_path}")
            return

        source_name = os.path.basename(raw_file_path).replace('.jsonl', '')
        processed_file_path = os.path.join(output_dir, f"processed_{source_name}.jsonl")
        temp_raw_path = raw_file_path + ".temp"

        print(f"\nProcessing {raw_file_path} -> {processed_file_path}")
        
        # Fast file loading (load all lines into memory to release file handle quickly)
        with open(raw_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        processed_count = 0
        skipped_count = 0

        last_processed_idx = -1
        try:
            async with aiohttp.ClientSession() as session:
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

                        # Execute Map-Reduce
                        source_id = data.get('source', 'unknown')
                        final_structured = await self.process_document(session, raw_text, source_id)
                        
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
        except BaseException:
            print("\n[!] Script interrupted. Gracefully saving progress...")
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
    parser = argparse.ArgumentParser(description="Async Map-Reduce JSONL Formatter for LM Studio.")
    parser.add_argument("--directory", default=os.path.join("data", "training"), help="Input directory")
    parser.add_argument("--output", default=os.path.join("data", "processed"), help="Output directory")
    parser.add_argument("--url", default="http://localhost:1234/v1", help="LM Studio URL")
    parser.add_argument("--concurrency", type=int, default=3, help="Max concurrent LLM requests")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    formatter = AsyncLocalFormatter(base_url=args.url, max_concurrent_requests=args.concurrency)
    
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
