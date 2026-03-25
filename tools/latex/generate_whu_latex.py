#!/usr/bin/env python3
"""Generate WHU LaTeX thesis snippets from the current markdown draft."""

from __future__ import annotations

import re
import shutil
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


def split_section_by_heading_pattern(block: str, heading_pattern: str) -> tuple[str, str]:
    m = re.search(heading_pattern, block, flags=re.MULTILINE)
    if not m:
        return block.strip(), ""
    return block[: m.start()].strip(), block[m.start() :].strip()


def split_first_h2_heading(block: str) -> tuple[str, str, str]:
    m = re.search(r"(?m)^##\s+(.+)$", block)
    if not m:
        return "", "", block.strip()
    heading = m.group(1).strip()
    start = m.start()
    next_m = re.search(r"(?m)^##\s+", block[m.end() :])
    if next_m:
        content_end = m.end() + next_m.start()
        content = block[m.end() :content_end].strip()
        rest = block[content_end:].strip()
    else:
        content = block[m.end() :].strip()
        rest = ""
    return heading, content, rest


def parse_tail_h2_sections(block: str) -> dict[str, tuple[str, str]]:
    sections: dict[str, tuple[str, str]] = {}
    matches = list(re.finditer(r"(?m)^##\s+(.+)$", block))
    if not matches:
        return sections
    for idx, match in enumerate(matches):
        heading = match.group(1).strip()
        content_start = match.end()
        content_end = matches[idx + 1].start() if idx + 1 < len(matches) else len(block)
        content = block[content_start:content_end].strip()
        heading_key = re.sub(r"\s+", "", heading)
        if heading_key.startswith("致谢"):
            sections["ack"] = (heading, content)
        elif heading_key.startswith("附录"):
            sections["appendix"] = (heading, content)
    return sections


def markdown_block_to_latex(block: str, *, shift_heading: int = 0) -> str:
    if not block.strip():
        return ""
    return run_pandoc(block.strip(), shift_heading=shift_heading).strip()


def build_unnumbered_section(
    title: str, block: str, *, add_toc: bool = True, clear_page: bool = False
) -> str:
    parts: list[str] = []
    if clear_page:
        parts.append("\\clearpage")
    parts.append(f"\\section*{{{title}}}")
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

    def ensure_ref_end_punct(text: str) -> str:
        t = text.strip()
        if not t:
            return t
        if t.endswith(("。", ".", "．")):
            return t
        return t + "."

    parts = [
        "\\clearpage",
        "\\renewcommand{\\refname}{参考文献}",
        "\\addcontentsline{toc}{section}{参考文献}",
        "\\begin{thebibliography}{99}",
        "\\setlength{\\itemsep}{0pt}",
        "\\setlength{\\parsep}{0pt}",
        "\\setlength{\\parskip}{0pt}",
    ]
    for idx, item in enumerate(items, 1):
        item = ensure_ref_end_punct(item)
        parts.append(f"\\bibitem{{ref{idx}}} {markdown_block_to_latex(wrap_urls(item))}")
    parts.append("\\end{thebibliography}")
    return "\n\n".join(parts).strip() + "\n"


def build_appendix_section(title: str, block: str) -> str:
    subtitle = ""
    m = re.match(r"^附\s*录\s*([A-Za-zＡ-Ｚ]?)(.*)$", title.strip())
    if m:
        label = m.group(1).strip()
        tail = re.sub(r"^[\s:：\-、.]+", "", m.group(2).strip())
        if label and tail:
            subtitle = f"{label} {tail}"
        elif tail:
            subtitle = tail
        elif label:
            subtitle = label
    body_md = block.strip()
    if subtitle:
        body_md = f"**{subtitle}**\n\n{body_md}"
    body_tex = markdown_block_to_latex(body_md)
    parts = [
        "\\clearpage",
        "\\section*{附 录}",
        "\\addcontentsline{toc}{section}{附录}",
    ]
    if body_tex:
        parts.append(body_tex)
    return "\n\n".join(parts).strip() + "\n"


def count_longtable_header_columns(first_head_block: str) -> int | None:
    top_idx = first_head_block.rfind("\\toprule")
    mid_idx = first_head_block.rfind("\\midrule")
    if top_idx < 0 or mid_idx < 0 or mid_idx <= top_idx:
        return None
    header_region = first_head_block[top_idx:mid_idx]
    return len(re.findall(r"(?<!\\)&", header_region)) + 1


def add_longtable_continued_labels(body_tex: str) -> str:
    longtable_pattern = re.compile(r"\\begin{longtable}.*?\\end{longtable}", re.DOTALL)

    def repl(match: re.Match[str]) -> str:
        block = match.group(0)
        if "\\endfirsthead" not in block or "\\endhead" not in block:
            return block
        if "\\caption{" not in block or "（续表" in block:
            return block
        first_head, rest = block.split("\\endfirsthead", 1)
        continuation_head, tail = rest.split("\\endhead", 1)
        col_count = count_longtable_header_columns(first_head)
        if not col_count:
            return block
        continued_label = f"\\multicolumn{{{col_count}}}{{r}}{{（续表\\thetable）}} \\\\\n"
        continuation_head = "\n" + continued_label + continuation_head.lstrip("\n")
        return first_head + "\\endfirsthead" + continuation_head + "\\endhead" + tail

    return longtable_pattern.sub(repl, body_tex)


MATH_INLINE_TOKENS = {
    "N",
    "D",
    "r",
    "L_k",
    "A_m",
    "B_m",
    "C_m",
    "Q_m",
    "S_m",
    "T_m(N)",
    "W_m(D,r)",
    "total_bytes",
    "elapsed_ms",
    "end_to_end_ms",
    "throughput_rps",
    "throughput_e2e_rps",
}


def format_mathish_inline(content: str) -> str | None:
    token = content.strip()
    if token in MATH_INLINE_TOKENS:
        return f"\\({token}\\)"
    if re.fullmatch(r"[A-Z]_[a-z](?:\([A-Za-z0-9,]+\))?", token):
        return f"\\({token}\\)"
    if re.fullmatch(r"[A-Za-z]=[0-9,]+", token):
        return f"\\({token}\\)"
    if re.fullmatch(r"[A-Za-z](?:≈|<=|>=|=)[0-9]+(?:～[0-9]+)?", token):
        token = token.replace("≈", "\\approx ")
        token = token.replace("<=", "\\le ")
        token = token.replace(">=", "\\ge ")
        token = token.replace("～", "\\text{～}")
        return f"\\({token}\\)"
    if re.fullmatch(r"[0-9]+/[0-9]+/[0-9]+/[0-9]+", token):
        return None
    return None


def normalize_inline_terms(body_tex: str) -> str:
    inline_code_pattern = re.compile(r"\\texttt\{((?:[^{}]|\\[{}%_&#$ ]|\\textbar\{\}|\\/\-?|\\\+|\\-|\\~)*)\}")

    def repl(match: re.Match[str]) -> str:
        content = match.group(1)
        content = content.replace("\\ ", " ")
        content = content.replace("-\\/-", "--")
        content = content.replace("\\/","/")
        content = content.replace("\\+","+") 
        content = content.replace("\\-", "-")
        content = re.sub(r"\s+", " ", content).strip()
        mathish = format_mathish_inline(content)
        if mathish:
            return mathish
        return f"\\thesisterm{{{content}}}"

    body_tex = inline_code_pattern.sub(repl, body_tex)
    body_tex = body_tex.replace("-\\textgreater{}", "\\textrightarrow{}")
    body_tex = body_tex.replace("\\thesisterm{5/10/15/20}", "5/10/15/20")
    body_tex = body_tex.replace("\\thesisterm{A\\_m/C\\_m/B\\_m/W\\_m}", "\\(A_m/C_m/B_m/W_m\\)")
    body_tex = body_tex.replace("经验交叉点 \\thesisterm{N*}", "经验交叉点 \\(N^*\\)")
    body_tex = body_tex.replace(
        "\\texttt{INITIALIZING\\ \\textrightarrow{}\\ INITIALIZED}",
        "\\thesisterm{INITIALIZING} \\textrightarrow{} \\thesisterm{INITIALIZED}",
    )
    body_tex = body_tex.replace(
        "\\texttt{binary\\_bytes\\ \\textless{}=\\ peer\\_bytes}",
        "\\(\\mathrm{binary\\_bytes} \\le \\mathrm{peer\\_bytes}\\)",
    )
    body_tex = body_tex.replace("\\texttt{N\\textless{}=100000}", "\\(N\\le 100000\\)")
    body_tex = body_tex.replace("\\texttt{N\\textgreater{}=10}", "\\(N\\ge 10\\)")
    body_tex = body_tex.replace("\\texttt{N\\textgreater{}=50}", "\\(N\\ge 50\\)")
    body_tex = body_tex.replace("\\texttt{binary\\ \\textless{}=\\ peer}", "\\(\\mathrm{binary} \\le \\mathrm{peer}\\)")
    body_tex = re.sub(r"\\thesisterm\{([A-Z])\\_m\}", lambda m: f"\\({m.group(1)}_m\\)", body_tex)
    body_tex = re.sub(
        r"\\thesisterm\{([A-Za-z])(?:≈|<=|>=|=)([0-9]+)(?:～([0-9]+))?\}",
        lambda m: "\\("
        + m.group(1)
        + (
            "\\approx "
            if "≈" in m.group(0)
            else "\\le "
            if "<=" in m.group(0)
            else "\\ge "
            if ">=" in m.group(0)
            else "="
        )
        + m.group(2)
        + (f"\\text{{～}}{m.group(3)}" if m.group(3) else "")
        + "\\)",
        body_tex,
    )
    return body_tex


def normalize_text_latex(body_tex: str) -> str:
    return normalize_inline_terms(body_tex)


def promote_chapter4_equations(body_tex: str) -> str:
    start_marker = "\\section{理论模型与数学推导}"
    end_marker = "\\section{实验迭代、对比与理论一致性分析}"
    start = body_tex.find(start_marker)
    end = body_tex.find(end_marker, start if start >= 0 else 0)
    if start < 0 or end < 0 or end <= start:
        return body_tex

    chapter4 = body_tex[start:end]

    def display_to_equation(match: re.Match[str]) -> str:
        content = match.group(1).strip("\n")
        return f"\\begin{{equation*}}\n{content}\n\\end{{equation*}}"

    chapter4 = re.sub(r"\\\[\s*\n(.*?)\n\s*\\\]", display_to_equation, chapter4, flags=re.DOTALL)

    labels = [
        ("S_m(N) = S_{0,m} + N B_m", "eq:space-linear"),
        ("S_{\\text{bin}}(N) = S_{\\text{shared}} + N \\left(8 + g_{\\text{bin}} + \\sum_k p_k \\widetilde{L}_k\\right)", "eq:bin-space-total"),
        ("N^*_{\\text{space}}(\\text{bin},x) = \\frac{S_{\\text{shared}} - S_{0,x}}{B_x - B_{\\text{bin}}}", "eq:space-crossover"),
        ("T_m(N) = A_m + N C_m", "eq:time-linear"),
        ("Q_m(N) = \\frac{1000N}{T_m(N)} = \\frac{1000}{C_m + A_m/N}", "eq:throughput"),
        ("N^*_{\\text{time}}(\\text{bin},x) = \\frac{A_{\\text{bin}} - A_x}{C_x - C_{\\text{bin}}}", "eq:time-crossover"),
        ("E_m(D,r) = A_{\\text{spawn}}(D) + A_m + rC_m + W_m(D,r)", "eq:multi-time"),
        ("D_{\\text{peak},m} = \\sqrt{\\frac{\\beta_m + rC_m}{\\gamma_m}}", "eq:device-peak"),
    ]

    for needle, label in labels:
        pattern = re.compile(
            rf"\\begin{{equation\*}}\s*(?P<body>(?:(?!\\end{{equation\*}}).)*?{re.escape(needle)}(?:(?!\\end{{equation\*}}).)*?)\\end{{equation\*}}",
            re.DOTALL,
        )
        chapter4 = pattern.sub(
            lambda match: "\\begin{equation}\n"
            + match.group("body").strip()
            + f"\n\\label{{{label}}}\n\\end{{equation}}",
            chapter4,
            count=1,
        )

    chapter4 = re.sub(
        r"\\begin{equation\*?}\n(.*?)\n\\end{equation\*?}",
        lambda match: match.group(0).split("\n", 1)[0] + "\n" + match.group(1).strip() + "\n" + match.group(0).rsplit("\n", 1)[-1],
        chapter4,
        flags=re.DOTALL,
    )
    chapter4 = re.sub(r"\n\s*\n(\\label\{)", r"\n\1", chapter4)
    chapter4 = re.sub(r"\n\s*\n\\end{equation\*}", r"\n\\end{equation*}", chapter4)
    chapter4 = re.sub(r"\n\s*\n\\end{equation}", r"\n\\end{equation}", chapter4)

    return body_tex[:start] + chapter4 + body_tex[end:]


def normalize_body_latex(body_tex: str) -> str:
    def convert_calc_width_expr(match: re.Match[str]) -> str:
        tabsep_count = float(match.group(1))
        ratio = float(match.group(2))
        return (
            "p{\\dimexpr "
            + f"{ratio:.6f}\\linewidth - {tabsep_count * ratio:.6f}\\tabcolsep\\relax"
            + "}"
        )

    # Convert pandoc width form `p{(\linewidth - n\tabcolsep) * \real{x}}`
    # into TeX-safe `\dimexpr` form to avoid table overflow and bad alignment.
    body_tex = re.sub(
        r"p\{\(\\linewidth\s*-\s*([0-9]+)\s*\\tabcolsep\)\s*\*\s*\\real\{([0-9.]+)\}\}",
        convert_calc_width_expr,
        body_tex,
    )

    replacements = {
        "\\begin{longtable}[]{@{}lrr@{}}": "\\begin{longtable}[]{@{}\n  >{\\raggedright\\arraybackslash}p{\\dimexpr0.18\\linewidth-0.72\\tabcolsep\\relax}\n  >{\\raggedleft\\arraybackslash}p{\\dimexpr0.41\\linewidth-1.64\\tabcolsep\\relax}\n  >{\\raggedleft\\arraybackslash}p{\\dimexpr0.41\\linewidth-1.64\\tabcolsep\\relax}@{}}",
        "\\begin{longtable}[]{@{}ccc@{}}": "\\begin{longtable}[]{@{}\n  >{\\centering\\arraybackslash}p{\\dimexpr0.16\\linewidth-0.64\\tabcolsep\\relax}\n  >{\\centering\\arraybackslash}p{\\dimexpr0.42\\linewidth-1.68\\tabcolsep\\relax}\n  >{\\centering\\arraybackslash}p{\\dimexpr0.42\\linewidth-1.68\\tabcolsep\\relax}@{}}",
        "\\begin{longtable}[]{@{}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2174}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2174}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2174}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2174}}\n  >{\\raggedright\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.1304}}@{}}": "\\begin{longtable}[]{@{}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.12}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.18}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.18}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.18}}\n  >{\\raggedright\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.34}}@{}}",
        "\\begin{longtable}[]{@{}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.1786}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.1786}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.1786}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.1786}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.1786}}\n  >{\\raggedright\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.1071}}@{}}": "\\begin{longtable}[]{@{}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.11}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.09}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.14}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.14}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.14}}\n  >{\\raggedright\\arraybackslash}p{(\\linewidth - 10\\tabcolsep) * \\real{0.38}}@{}}",
        "\\begin{longtable}[]{@{}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2000}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2000}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2000}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2000}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.2000}}@{}}": "\\begin{longtable}[]{@{}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.16}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.12}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.24}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.24}}\n  >{\\centering\\arraybackslash}p{(\\linewidth - 8\\tabcolsep) * \\real{0.24}}@{}}",
    }
    for src, dst in replacements.items():
        body_tex = body_tex.replace(src, dst)
    # Keep table header minipages constrained to the column width.
    body_tex = body_tex.replace("\\begin{minipage}[b]{\\linewidth}", "\\begin{minipage}[t]{\\hsize}")
    body_tex = body_tex.replace("\\begin{minipage}[t]{\\hsize}\\raggedright", "\\begin{minipage}[t]{\\hsize}\\centering")
    body_tex = normalize_inline_terms(body_tex)
    body_tex = add_longtable_continued_labels(body_tex)
    body_tex = promote_chapter4_equations(body_tex)
    return body_tex


def convert_svg_assets(body_tex: str) -> None:
    converter = shutil.which("rsvg-convert")
    if not converter:
        print("warning: rsvg-convert not found; SVG assets will not be converted to PDF")
        return

    svg_refs = sorted(set(re.findall(r"\\includesvg(?:\[[^\]]*\])?\{([^}]+\.svg)\}", body_tex)))
    for ref in svg_refs:
        svg_path = Path(ref)
        if not svg_path.is_absolute():
            svg_path = (ROOT / svg_path).resolve()
        if not svg_path.exists():
            print(f"warning: svg asset not found: {svg_path}")
            continue
        pdf_path = svg_path.with_suffix(".pdf")
        if pdf_path.exists() and pdf_path.stat().st_mtime >= svg_path.stat().st_mtime:
            continue
        pdf_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            [converter, "-f", "pdf", "-o", str(pdf_path), str(svg_path)],
            check=True,
            cwd=ROOT,
        )


def main() -> None:
    text = MD_PATH.read_text(encoding="utf-8")

    cn_abs_block = extract_between(text, "## 摘  要", "## ABSTRACT")
    en_abs_block = extract_between(text, "## ABSTRACT", "## 1 绪论")
    body_block = text[text.find("## 1 绪论") :].strip()
    body_block = strip_heading_numbers(body_block)
    body_main, rest = split_section(body_block, "参考文献")
    refs_block, rest = split_next_h2(rest)
    ack_block = ""
    appendix_title = ""
    appendix_block = ""
    if rest:
        tail_sections = parse_tail_h2_sections(rest)
        if "ack" in tail_sections:
            _ack_title, ack_block = tail_sections["ack"]
        if "appendix" in tail_sections:
            appendix_title, appendix_block = tail_sections["appendix"]

    cn_abs_md, cn_keywords = extract_keywords(cn_abs_block, "关键词")
    en_abs_md, en_keywords = extract_keywords(en_abs_block, "Keywords")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    (OUT_DIR / "cn_abstract.tex").write_text(normalize_text_latex(run_pandoc(cn_abs_md)), encoding="utf-8")
    (OUT_DIR / "en_abstract.tex").write_text(normalize_text_latex(run_pandoc(en_abs_md)), encoding="utf-8")
    body_parts = [normalize_body_latex(markdown_block_to_latex(body_main, shift_heading=-1))]
    if refs_block:
        body_parts.append(build_references_section(refs_block))
    if ack_block:
        body_parts.append(build_unnumbered_section("致 谢", ack_block, clear_page=True))
    if appendix_block:
        appendix_title = appendix_title or "附 录"
        body_parts.append(build_appendix_section(appendix_title, appendix_block))
    body_tex = "\n\n".join(x.strip() for x in body_parts if x.strip()) + "\n"
    convert_svg_assets(body_tex)
    (OUT_DIR / "body.tex").write_text(body_tex, encoding="utf-8")
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
