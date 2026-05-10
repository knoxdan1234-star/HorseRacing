"""Convert the Traditional Chinese horse racing guide to PDF via Chrome headless."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MD = ROOT / "output" / "guide" / "horse_racing_guide_tc.md"
HTML = ROOT / "output" / "guide" / "horse_racing_guide_tc.html"
PDF = ROOT / "output" / "guide" / "horse_racing_guide_tc.pdf"

CSS = """
@page { size: A4; margin: 20mm 18mm; }
html { font-size: 11pt; }
body {
  font-family: "PingFang TC", "Heiti TC", "Hiragino Sans CNS",
               "Noto Sans CJK TC", "Microsoft JhengHei", sans-serif;
  line-height: 1.75;
  color: #222;
  max-width: 760px;
  margin: 0 auto;
}
h1 { font-size: 26pt; border-bottom: 3px solid #b22222; padding-bottom: 8px; color: #b22222; }
h2 { font-size: 18pt; border-bottom: 1px solid #888; padding-bottom: 4px; margin-top: 2em; color: #333; }
h3 { font-size: 14pt; margin-top: 1.5em; color: #444; }
h4 { font-size: 12pt; margin-top: 1.2em; color: #555; }
blockquote {
  border-left: 4px solid #b22222;
  background: #fdf3f3;
  padding: 8px 14px;
  margin: 1em 0;
  color: #555;
}
code { background: #f4f4f4; padding: 1px 5px; border-radius: 3px; font-size: 0.92em; }
pre { background: #f4f4f4; padding: 10px; border-radius: 4px; overflow-x: auto; }
table { border-collapse: collapse; width: 100%; margin: 1em 0; }
th, td { border: 1px solid #bbb; padding: 6px 10px; text-align: left; }
th { background: #f0f0f0; }
hr { border: none; border-top: 1px solid #ccc; margin: 2em 0; }
ul, ol { margin: 0.4em 0 0.8em 1.4em; }
a { color: #b22222; text-decoration: none; }
"""


def md_to_html():
    html_body = subprocess.check_output(
        ["pandoc", str(MD), "-f", "markdown", "-t", "html5", "--no-highlight"],
        text=True,
    )
    full = f"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<title>賽馬完全入門指南</title>
<style>{CSS}</style>
</head>
<body>
{html_body}
</body>
</html>
"""
    HTML.write_text(full, encoding="utf-8")
    print(f"Wrote HTML: {HTML}")


def html_to_pdf():
    chrome = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    subprocess.check_call([
        chrome,
        "--headless",
        "--disable-gpu",
        "--no-pdf-header-footer",
        f"--print-to-pdf={PDF}",
        f"file://{HTML}",
    ])
    print(f"Wrote PDF: {PDF}  ({PDF.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    md_to_html()
    html_to_pdf()
