#!/usr/bin/env python3
"""Generate WHU LaTeX thesis snippets from the current markdown draft."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MD_PATH = ROOT / "毕业论文初稿.md"
OUT_DIR = ROOT / "thesis_latex" / "generated"


def run_pandoc(md_text: str, shift_heading: int = 0) -> str:
    cmd = [
        "pandoc",
        "-f",
        "gfm+tex_math_dollars",
        "-t",
        "latex",
        "--wrap=none",
    ]
    if shift_heading != 0:
        cmd.append(f"--shift-heading-level-by={shift_heading}")
    res = subprocess.run(
        cmd,
        input=md_text,
        text=True,
        capture_output=True,
        check=True,
        cwd=ROOT,
    )
    return res.stdout.strip() + "\n"


def extract_between(text: str, start: str, end: str) -> str:
    s = text.find(start)
    if s < 0:
        raise ValueError(f"Cannot find marker: {start}")
    s += len(start)
    e = text.find(end, s)
    if e < 0:
        raise ValueError(f"Cannot find end marker: {end}")
    return text[s:e].strip()


def extract_keywords(block: str, prefix: str) -> tuple[str, str]:
    keywords = ""
    content_lines: list[str] = []
    for line in block.splitlines():
        s = line.strip()
        if re.fullmatch(r"-{3,}", s):
            continue
        if s.startswith(prefix):
            m = re.match(rf"^{re.escape(prefix)}\s*[:：]\s*(.*)$", s)
            if m:
                keywords = m.group(1).strip()
            else:
                keywords = s[len(prefix) :].strip()
            continue
        content_lines.append(s)
    return "\n".join(content_lines).strip(), keywords


def main() -> None:
    text = MD_PATH.read_text(encoding="utf-8")

    cn_abs_block = extract_between(text, "## 摘  要", "## ABSTRACT")
    en_abs_block = extract_between(text, "## ABSTRACT", "## 1 绪论")
    body_block = text[text.find("## 1 绪论") :].strip()

    cn_abs_md, cn_keywords = extract_keywords(cn_abs_block, "关键词")
    en_abs_md, en_keywords = extract_keywords(en_abs_block, "Keywords")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    (OUT_DIR / "cn_abstract.tex").write_text(run_pandoc(cn_abs_md), encoding="utf-8")
    (OUT_DIR / "en_abstract.tex").write_text(run_pandoc(en_abs_md), encoding="utf-8")
    (OUT_DIR / "body.tex").write_text(run_pandoc(body_block, shift_heading=-1), encoding="utf-8")
    (OUT_DIR / "keywords.tex").write_text(cn_keywords + "\n", encoding="utf-8")
    (OUT_DIR / "keywords_en.tex").write_text(en_keywords + "\n", encoding="utf-8")

    print("Generated:")
    for p in [
        OUT_DIR / "cn_abstract.tex",
        OUT_DIR / "en_abstract.tex",
        OUT_DIR / "body.tex",
        OUT_DIR / "keywords.tex",
        OUT_DIR / "keywords_en.tex",
    ]:
        print(" -", p)


if __name__ == "__main__":
    main()
