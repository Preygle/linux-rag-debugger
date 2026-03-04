import os
import json
import argparse
from typing import Dict, Any

class NormalFormatter:
    """
    A non-LLM heuristic formatter that attempts to map raw JSON structures
    into the strict RAG schema without using API tokens.
    """
    def __init__(self):
        pass

    def extract_domain(self, text: str) -> str:
        text_lower = text.lower()
        if "network" in text_lower or "ip" in text_lower or "ping" in text_lower or "wifi" in text_lower:
            return "networking"
        elif "boot" in text_lower or "grub" in text_lower or "kernel" in text_lower or "uefi" in text_lower:
            return "kernel/boot"
        elif "systemd" in text_lower or "service" in text_lower or "systemctl" in text_lower:
            return "systemd"
        elif "apt" in text_lower or "dpkg" in text_lower or "pacman" in text_lower or "install" in text_lower:
            return "package_manager"
        elif "disk" in text_lower or "mount" in text_lower or "ext4" in text_lower or "btrfs" in text_lower or "fstab" in text_lower:
            return "filesystem"
        return "general"

    def format_document(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Attempts to heuristically parse standard forum/chat JSON into the RAG schema.
        """
        # Determine root text
        raw_text = ""
        threads = []
        
        if 'messages' in data:
            raw_text = "\n".join([f"{m['role']}: {m['content']}" for m in data['messages']])
            threads = [m['content'] for m in data['messages']]
        elif 'text' in data:
            raw_text = data['text']
            threads = [raw_text]
            
        doc_id = data.get('source', 'unknown_doc')

        # 1. Domain
        domain = self.extract_domain(raw_text)

        # 2. Problem (Usually the first message or title)
        problem = data.get('title', '')
        if not problem and threads:
            problem = threads[0][:500].replace('\n', ' ') + "..."

        # 3. Raw Logs (Look for code blocks or standard terminal outputs)
        raw_logs_blocks = []
        in_block = False
        current_block = []
        
        for line in raw_text.split('\n'):
            if line.strip().startswith('```'):
                in_block = not in_block
                if not in_block and current_block:
                    raw_logs_blocks.append('\n'.join(current_block))
                    current_block = []
                continue
                
            if in_block:
                current_block.append(line)
            # Hardcoded heuristics for typical bash signs without codeblocks
            elif "error:" in line.lower() or "exception" in line.lower() or "traceback" in line.lower() or "fatal" in line.lower() or line.startswith('$ ') or line.startswith('# '):
                raw_logs_blocks.append(line)

        raw_logs = "\n...\n".join(raw_logs_blocks) if raw_logs_blocks else ""

        # 4. Hardware/Env 
        # Very simple heuristic: extract words like Ubuntu, Debian, x86_64, versions
        specs = []
        for word in ["ubuntu", "debian", "arch", "fedora", "centos", "x86_64", "arm64", "kernel", "systemd", "gnome", "kde"]:
            if word in raw_text.lower():
                specs.append(word)
        hardware_env = ", ".join(set(specs)) if specs else "unknown"

        # 5. Solution (Usually the last message)
        solution = "Unresolved"
        if len(threads) > 1:
            solution = threads[-1][:1000].replace('\n', ' ')

        # Final Payload Match
        return {
            "doc_id": doc_id,
            "domain": domain,
            "hardware_env": hardware_env,
            "problem": problem.strip(),
            "raw_logs": raw_logs.strip(),
            "solution": solution.strip(),
            "source_file": "heuristically_parsed",
            "original_link": doc_id
        }

    def process_file(self, raw_file_path: str, output_dir: str):
        if not os.path.exists(raw_file_path):
            print(f"File not found: {raw_file_path}")
            return

        source_name = os.path.basename(raw_file_path).replace('.jsonl', '')
        processed_file_path = os.path.join(output_dir, f"processed_{source_name}.jsonl")
        
        print(f"\n[Heuristic] Processing {raw_file_path} -> {processed_file_path}")
        
        processed_count = 0
        skipped_count = 0
        
        # Read file
        with open(raw_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        with open(processed_file_path, 'w', encoding='utf-8') as out_f:
            for line in lines:
                if not line.strip(): continue
                
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                if data.get("normal_formatting_done", False):
                    skipped_count += 1
                    continue
                
                structured = self.format_document(data)
                out_f.write(json.dumps(structured) + '\n')
                processed_count += 1
                print(f"  [✓] Processed: {data.get('source', 'unknown')} | Total: {processed_count}", end='\r')
                
        print(f"\nFinished {source_name} | Mapped: {processed_count} | Skipped: {skipped_count}")

def main():
    parser = argparse.ArgumentParser(description="Normal Heuristic JSONL Formatter (No LLM).")
    parser.add_argument("--directory", default=os.path.join("data", "training"), help="Input directory")
    parser.add_argument("--output", default=os.path.join("data", "processed_normal"), help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    formatter = NormalFormatter()
    
    if os.path.exists(args.directory):
        for file in os.listdir(args.directory):
            if file.endswith(".jsonl") and not file.startswith("processed_"):
                formatter.process_file(os.path.join(args.directory, file), args.output)
    else:
        print(f"Directory {args.directory} does not exist.")

if __name__ == "__main__":
    main()
