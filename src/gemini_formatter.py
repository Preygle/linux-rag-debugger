import os
import json
import time
import argparse
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

class GeminiFormatter:
    def __init__(self, rpm_limit=15):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key or self.api_key == "your_gemini_api_key_here":
            raise ValueError("GEMINI_API_KEY is not set correctly in .env")
        
        # Initialize new SDK client
        self.client = genai.Client(api_key=self.api_key)
        self.model_name = 'gemini-2.0-flash'
        self.sleep_time = 60.0 / rpm_limit

    def format_text(self, raw_text: str) -> str:
        # Prevent massive documents from blowing the 1M Token Per Minute free tier quota
        if len(raw_text) > 30000:
            print(f"    Warning: Truncating huge text ({len(raw_text)} chars -> 30000)")
            raw_text = raw_text[:30000] + "\n...[TRUNCATED_TO_SAVE_TOKENS]..."
            
        prompt = f"""
        You are a technical data cleaner preparing text for a Retrieval-Augmented Generation (RAG) system focused on Linux log diagnosis.
        Below is raw scraped text from a forum, bug tracker, or wiki. 
        
        Your task is to:
        1. Extract the core problem, symptom, or error being discussed.
        2. Detail the exact steps taken to troubleshoot or fix it.
        3. Remove non-informative noise (like greetings, signatures, unrelated thread chatter).
        4. State clearly what worked or the final resolution.
        
        Return pure text. Do NOT wrap it in markdown code blocks like ```markdown.
        
        Raw Text:
        {raw_text}
        """
        
        for attempt in range(3):
            try:
                response = self.client.models.generate_content(
                    model=self.model_name,
                    contents=prompt
                )
                time.sleep(self.sleep_time) # Rate limit to stay within RPM
                if response.text:
                    return response.text.replace("```markdown", "").replace("```", "").strip()
                return None
            except Exception as e:
                print(f"Error calling Gemini: {e}")
                # Wait longer on error (e.g., rate limit hit)
                time.sleep(10 + (attempt * 10))
                
        return None

    def process_file(self, file_path: str):
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return

        print(f"\nProcessing file: {file_path}")
        temp_path = file_path + ".temp"
        
        processed_count = 0
        skipped_count = 0
        error_count = 0

        with open(file_path, 'r', encoding='utf-8') as infile, \
             open(temp_path, 'w', encoding='utf-8') as outfile:
            
            for line in infile:
                if not line.strip(): continue
                
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    print("Skipping malformed JSON line.")
                    continue
                
                # Ensure flag gets checked before doing any work
                if data.get("formatting_done", False) is True:
                    outfile.write(json.dumps(data) + '\n')
                    skipped_count += 1
                    continue
                
                # Extract text to format
                raw_text = ""
                if 'messages' in data: # SFT Format (StackExchange)
                    raw_text = "\n".join([f"{m['role']}: {m['content']}" for m in data['messages']])
                elif 'text' in data:   # Raw Format (GitHub/Wiki)
                    raw_text = data['text']
                
                if not raw_text:
                    outfile.write(json.dumps(data) + '\n')
                    continue

                formatted_text = self.format_text(raw_text)
                
                if formatted_text:
                    # Replace with cleaned text and set the flag
                    if 'messages' in data:
                        data['text'] = formatted_text # Standardize on 'text'
                        del data['messages']
                    elif 'text' in data:
                        data['text'] = formatted_text
                        
                    data['formatting_done'] = True
                    outfile.write(json.dumps(data) + '\n')
                    processed_count += 1
                    print(f"  [{processed_count}] Formatted document from source: {data.get('source', 'unknown')}")
                else:
                    # Write original if formatting failed so we don't drop data
                    outfile.write(json.dumps(data) + '\n')
                    error_count += 1

        # Replace original file safely
        try:
            os.replace(temp_path, file_path)
            print(f"Finished {file_path} -> Formatted: {processed_count} | Skipped (already done): {skipped_count} | Errors: {error_count}")
        except Exception as e:
            print(f"Could not overwrite {file_path}. Data saved to {temp_path}. Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean and structure JSONL training data using Gemini (No Vector DB insertion).")
    parser.add_argument("--directory", default=os.path.join("data", "training"), help="Directory containing JSONL files")
    parser.add_argument("--rpm", type=int, default=15, help="Requests per minute (15 = free tier safe)")
    args = parser.parse_args()

    formatter = GeminiFormatter(rpm_limit=args.rpm)
    
    if os.path.exists(args.directory):
        for file in os.listdir(args.directory):
            if file.endswith(".jsonl"):
                formatter.process_file(os.path.join(args.directory, file))
    else:
        print(f"Directory {args.directory} does not exist.")
