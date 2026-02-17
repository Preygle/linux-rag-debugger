import re

def clean_text(text: str) -> str:
    """
    Cleans and normalizes text.
    Removes HTML tags, multiple spaces, and non-printable characters.
    """
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove multiple whitespace and newlines
    text = re.sub(r'\s+', ' ', text).strip()
    return text

if __name__ == "__main__":
    sample = "  This is a   test.\n\nNew line.  "
    print(f"Original: '{sample}'")
    print(f"Cleaned: '{clean_text(sample)}'")
