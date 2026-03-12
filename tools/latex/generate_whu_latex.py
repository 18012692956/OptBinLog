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
        "markdown+tex_math_dollars+raw_tex",
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


def strip_heading_numbers(block: str) -> str:
    lines: list[str] = []
    for line in block.splitlines():
        m = re.match(r"^(#{1,6})\s+(\d+(?:\.\d+)*)\s+(.*)$", line)
        if m:
            line = f"{m.group(1)} {m.group(3)}"
        lines.append(line)
    return "\n".join(lines).strip()


def split_section(block: str, heading: str) -> tuple[str, str]:
    marker = f"\n## {heading}\n"
    idx = block.find(marker)
    if idx < 0:
        return block.strip(), ""
    return block[:idx].strip(), block[idx + len(marker) :].strip()


def split_next_h2(block: str) -> tuple[str, str]:
    m = re.search(r"(?m)^##\s+", block)
    if not m:
        return block.strip(), ""
    return block[: m.start()].strip(), block[m.start() :].strip()


def markdown_block_to_latex(block: str, *, shift_heading: int = 0) -> str:
    if not block.strip():
        return ""
    return run_pandoc(block.strip(), shift_heading=shift_heading).strip()


def build_unnumbered_section(title: str, block: str, *, add_toc: bool = True) -> str:
    parts = [f"\\section*{{{title}}}"]
    if add_toc:
        parts.append(f"\\addcontentsline{{toc}}{{section}}{{{title}}}")
    body_tex = markdown_block_to_latex(block)
    if body_tex:
        parts.append(body_tex)
    return "\n\n".join(parts).strip() + "\n"


def build_references_section(block: str) -> str:
    def wrap_urls(text: str) -> str:
        def repl(match: re.Match[str]) -> str:
            url = match.group(1)
            suffix = match.group(2) or ""
            return f"<{url}>{suffix}"

        return re.sub(r"(https?://[^\s>]+?)([.,])?(?=\s|$)", repl, text)

    items: list[str] = []
    current = ""
    for line in block.splitlines():
        s = line.strip()
        if not s or re.fullmatch(r"-{3,}", s):
            continue
        m = re.match(r"^\[(\d+)\]\s*(.*)$", s)
        if m:
            if current:
                items.append(current.strip())
            current = m.group(2).strip()
        else:
            current = (current + " " + s).strip()
    if current:
        items.append(current.strip())

    parts = [
        "\\renewcommand{\\refname}{参考文献}",
        "\\addcontentsline{toc}{section}{参考文献}",
        "\\begin{thebibliography}{99}",
        "\\setlength{\\itemsep}{0.2\\baselineskip}",
    ]
    for idx, item in enumerate(items, 1):
        parts.append(f"\\bibitem{{ref{idx}}} {markdown_block_to_latex(wrap_urls(item))}")
    parts.append("\\end{thebibliography}")
    return "\n\n".join(parts).strip() + "\n"


def normalize_body_latex(body_tex: str) -> str:
    replacements = {
        "\\begin{longtable}[]{@{}lrr@{}}": "\\begin{longtable}[]{@{}\n  >{\\raggedright\\arraybackslash}p{(\\linewidth - 4\\tabcolsep) * \\real{0.18}}\n  >{\\raggedleft\\arraybackslash}p{(\\linewidth - 4\\tabcolsep) * \\real{0.41}}\n  >{\\raggedleft\\arraybackslash}p{(\\linewidth - 4\\tabcolsep) * \\real{0.41}}@{}}",
    }
    for src, dst in replacements.items():
        body_tex = body_tex.replace(src, dst)
    return body_tex


def main() -> None:
    text = MD_PATH.read_text(encoding="utf-8")

    cn_abs_block = extract_between(text, "## 摘  要", "## ABSTRACT")
    en_abs_block = extract_between(text, "## ABSTRACT", "## 1 绪论")
    body_block = text[text.find("## 1 绪论") :].strip()
    body_block = strip_heading_numbers(body_block)
    body_main, rest = split_section(body_block, "参考文献")
    refs_block, rest = split_next_h2(rest)
    ack_block = ""
    appendix_block = ""
    if rest:
        ack_block, rest = split_section(rest, "附 录A 关键复现实验命令（当前保留结果）")
        if ack_block:
            ack_block = ack_block.removeprefix("## 致  谢").strip()
        appendix_block = rest
        if appendix_block:
            appendix_block = appendix_block.removeprefix("## 附 录A 关键复现实验命令（当前保留结果）").strip()

    cn_abs_md, cn_keywords = extract_keywords(cn_abs_block, "关键词")
    en_abs_md, en_keywords = extract_keywords(en_abs_block, "Keywords")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    (OUT_DIR / "cn_abstract.tex").write_text(run_pandoc(cn_abs_md), encoding="utf-8")
    (OUT_DIR / "en_abstract.tex").write_text(run_pandoc(en_abs_md), encoding="utf-8")
    body_parts = [normalize_body_latex(markdown_block_to_latex(body_main, shift_heading=-1))]
    if refs_block:
        body_parts.append(build_references_section(refs_block))
    if ack_block:
        body_parts.append(build_unnumbered_section("致 谢", ack_block))
    if appendix_block:
        body_parts.append(build_unnumbered_section("附 录A 关键复现实验命令（当前保留结果）", appendix_block))
    (OUT_DIR / "body.tex").write_text("\n\n".join(x.strip() for x in body_parts if x.strip()) + "\n", encoding="utf-8")
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
