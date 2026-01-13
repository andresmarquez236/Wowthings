
import sys

# Mock content data
data_plain = "Some plain text feature."
data_html = "<ul><li>Feature 1</li></ul>"
data_empty = ""

def _ensure_html(text):
    if not text: return ""
    text = text.strip()
    if text.startswith("<p") or text.startswith("<ul") or text.startswith("<ol") or text.startswith("<h"):
        return text
    return f"<p>{text}</p>"

# Test cases
try:
    assert _ensure_html(data_plain) == "<p>Some plain text feature.</p>"
    assert _ensure_html(data_html) == "<ul><li>Feature 1</li></ul>"
    assert _ensure_html(data_empty) == ""
    print("✅ HTML wrapping logic verified.")
except AssertionError as e:
    print(f"❌ HTML wrapping logic failed: {e}")
    sys.exit(1)
