import os
import json
import requests
from typing import Optional, Generator

LM_STUDIO_BASE_URL = os.getenv("LM_STUDIO_BASE_URL", "http://localhost:1234/v1")
LM_STUDIO_CHAT_MODEL = os.getenv(
    "LM_STUDIO_CHAT_MODEL",
    "bartowski/Meta-Llama-3.1-8B-Instruct-GGUF/Meta-Llama-3.1-8B-Instruct-Q6_K_L.gguf"
)


class LLMClient:
    def __init__(
        self,
        provider: str = "lmstudio",
        model_name: str = None,
        api_key: str = None,
        base_url: str = None,
    ):
        self.provider = provider
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")

        if provider == "lmstudio":
            self.base_url = base_url or LM_STUDIO_BASE_URL
            self.model_name = model_name or LM_STUDIO_CHAT_MODEL
        elif provider == "ollama":
            self.base_url = base_url or "http://localhost:11434"
            self.model_name = model_name or "llama2"
        elif provider == "openai":
            self.base_url = base_url or "https://api.openai.com/v1"
            self.model_name = model_name or "gpt-3.5-turbo"
        else:
            self.base_url = base_url
            self.model_name = model_name

    def generate(self, prompt: str) -> str:
        """
        Generates text using the configured LLM provider.
        """
        if self.provider == "lmstudio":
            return self._generate_openai_compat(prompt, timeout=300)
        elif self.provider == "ollama":
            return self._generate_ollama(prompt)
        elif self.provider == "openai":
            return self._generate_openai_compat(prompt, timeout=60)
        else:
            return f"Error: Unsupported provider '{self.provider}'."

    def _generate_openai_compat(self, prompt: str, timeout: int = 120) -> str:
        """Shared implementation for LM Studio and OpenAI (both use /chat/completions)."""
        url = f"{self.base_url.rstrip('/')}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
        }
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=timeout)
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except requests.exceptions.ConnectionError:
            return (
                f"[LLMClient] Cannot connect to {self.provider} at {self.base_url}. "
                "Make sure the server is running and the model is loaded."
            )
        except requests.exceptions.RequestException as e:
            return f"[LLMClient] Request failed: {e}"

    def _generate_ollama(self, prompt: str) -> str:
        url = f"{self.base_url}/api/generate"
        payload = {"model": self.model_name, "prompt": prompt, "stream": False}
        try:
            response = requests.post(url, json=payload, timeout=120)
            response.raise_for_status()
            return response.json().get("response", "")
        except requests.exceptions.RequestException as e:
            return f"[LLMClient] Error communicating with Ollama: {e}"


    def stream_generate(self, prompt: str) -> Generator[str, None, None]:
        """
        Yields text chunks in real-time from LM Studio's streaming API.
        Only supported for 'lmstudio' and 'openai' providers.
        """
        if self.provider not in ("lmstudio", "openai"):
            # Fallback: yield the whole response at once
            yield self.generate(prompt)
            return

        url = f"{self.base_url.rstrip('/')}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3,
            "stream": True,
        }

        try:
            with requests.post(
                url, headers=headers, json=payload,
                stream=True, timeout=300
            ) as resp:
                resp.raise_for_status()
                for raw_line in resp.iter_lines():
                    if not raw_line:
                        continue
                    line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
                    if not line.startswith("data: "):
                        continue
                    payload_str = line[6:].strip()
                    if payload_str == "[DONE]":
                        break
                    try:
                        chunk = json.loads(payload_str)
                        delta = chunk["choices"][0]["delta"].get("content", "")
                        if delta:
                            yield delta
                    except (json.JSONDecodeError, KeyError, IndexError):
                        continue
        except requests.exceptions.ConnectionError:
            yield (
                f"[LLMClient] Cannot connect to {self.provider} at {self.base_url}. "
                "Make sure LM Studio is running and the model is loaded."
            )
        except requests.exceptions.RequestException as e:
            yield f"[LLMClient] Request failed: {e}"


if __name__ == "__main__":
    client = LLMClient(provider="lmstudio")
    print(f"Using model: {client.model_name}")
    print("Streaming: ", end="", flush=True)
    for chunk in client.stream_generate("What is a kernel panic? Answer in 2 sentences."):
        print(chunk, end="", flush=True)
    print()
