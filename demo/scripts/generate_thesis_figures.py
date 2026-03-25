#!/usr/bin/env python3
import html
import os
from typing import Iterable, List, Sequence, Tuple


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "results", "paper_dataset", "supplement", "figures")

FONT = '"Avenir Next","Helvetica Neue",Arial,"PingFang SC","Noto Sans CJK SC",sans-serif'
INK = "#1F3446"
SUB = "#697C8D"
LINE = "#C6D0D8"
BLUE = "#F5F8FB"
GREEN = "#F7FAFB"
SAND = "#FAF8F4"
MIST = "#FBFCFD"
WHITE = "#FFFFFF"
NAVY = "#49657D"
TEAL = "#49657D"
GOLD = "#49657D"


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def esc(text: str) -> str:
    return html.escape(text, quote=True)


def wrap_text(text: str, max_chars: int) -> List[str]:
    words = text.split()
    if not words:
        return [""]
    lines: List[str] = []
    cur = words[0]
    for word in words[1:]:
        nxt = f"{cur} {word}"
        if len(nxt) <= max_chars:
            cur = nxt
        else:
            lines.append(cur)
            cur = word
    lines.append(cur)
    return lines


def wrap_paragraphs(paragraphs: Sequence[str], max_chars: int) -> List[str]:
    out: List[str] = []
    for paragraph in paragraphs:
        out.extend(wrap_text(paragraph, max_chars))
    return out


def svg_header(width: int, height: int, title: str, subtitle: str) -> List[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<defs>",
        '<marker id="arrow" markerWidth="12" markerHeight="12" refX="10" refY="6" orient="auto" markerUnits="strokeWidth">',
        f'<path d="M0,0 L12,6 L0,12 z" fill="{INK}"/>',
        "</marker>",
        "</defs>",
        f'<rect width="{width}" height="{height}" fill="{WHITE}"/>',
        f"<style>"
        f".title{{font-family:{FONT};font-size:30px;font-weight:700;fill:{INK};}}"
        f".subtitle{{font-family:{FONT};font-size:13px;fill:{SUB};}}"
        f".panel{{font-family:{FONT};font-size:16px;font-weight:700;fill:{INK};}}"
        f".cardtitle{{font-family:{FONT};font-size:16px;font-weight:700;fill:{INK};}}"
        f".cardtext{{font-family:{FONT};font-size:13px;fill:{INK};}}"
        f".meta{{font-family:{FONT};font-size:12px;fill:{SUB};}}"
        f".badge{{font-family:{FONT};font-size:12px;font-weight:700;fill:{WHITE};}}"
        f".dyn{{font-family:{FONT};}}"
        f"</style>",
    ]


def svg_footer(lines: List[str]) -> str:
    lines.append("</svg>")
    return "\n".join(lines)


def add_text(lines: List[str], x: float, y: float, text: str, cls: str, anchor: str = "start") -> None:
    lines.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" class="{cls}">{esc(text)}</text>')


def add_multiline(
    lines: List[str],
    x: float,
    y: float,
    items: Sequence[str],
    cls: str,
    anchor: str = "start",
    line_h: float = 22,
) -> None:
    lines.append(f'<text x="{x}" y="{y}" text-anchor="{anchor}" class="{cls}">')
    for idx, item in enumerate(items):
        dy = "0" if idx == 0 else str(line_h)
        lines.append(f'<tspan x="{x}" dy="{dy}">{esc(item)}</tspan>')
    lines.append("</text>")


def add_text_sized(
    lines: List[str],
    x: float,
    y: float,
    text: str,
    font_size: float,
    font_weight: int = 400,
    fill: str = INK,
    anchor: str = "start",
) -> None:
    lines.append(
        f"<text x=\"{x}\" y=\"{y}\" text-anchor=\"{anchor}\" class=\"dyn\" style='font-size:{font_size}px;"
        f"font-weight:{font_weight};fill:{fill};'>{esc(text)}</text>"
    )


def add_multiline_sized(
    lines: List[str],
    x: float,
    y: float,
    items: Sequence[str],
    font_size: float,
    font_weight: int = 400,
    fill: str = INK,
    anchor: str = "start",
    line_h: float | None = None,
) -> None:
    actual_lh = line_h if line_h is not None else round(font_size * 1.32, 2)
    lines.append(
        f"<text x=\"{x}\" y=\"{y}\" text-anchor=\"{anchor}\" class=\"dyn\" style='font-size:{font_size}px;"
        f"font-weight:{font_weight};fill:{fill};'>"
    )
    for idx, item in enumerate(items):
        dy = "0" if idx == 0 else str(actual_lh)
        lines.append(f'<tspan x="{x}" dy="{dy}">{esc(item)}</tspan>')
    lines.append("</text>")


def fit_wrapped_lines(
    paragraphs: Sequence[str],
    width: float,
    max_height: float,
    base_size: float,
    min_size: float,
    char_factor: float,
    line_ratio: float,
) -> Tuple[List[str], float, float]:
    size = base_size
    best_lines: List[str] = []
    while size >= min_size:
        max_chars = max(8, int(width / max(size * char_factor, 1)))
        best_lines = wrap_paragraphs(paragraphs, max_chars)
        line_h = round(size * line_ratio, 2)
        if len(best_lines) * line_h <= max_height:
            return best_lines, size, line_h
        size = round(size - 0.5, 2)
    line_h = round(min_size * line_ratio, 2)
    max_chars = max(8, int(width / max(min_size * char_factor, 1)))
    return wrap_paragraphs(paragraphs, max_chars), min_size, line_h


def fit_card_copy(
    title: str,
    body: Sequence[str],
    width: float,
    height: float,
    badge: str,
) -> Tuple[List[str], float, float, List[str], float, float]:
    has_body = any(item.strip() for item in body)
    title_size = 16.0
    body_size = 13.0
    title_min = 14.0
    body_min = 12.0
    title_width = width - (82 if badge else 36)
    body_width = width - 36
    if not has_body:
        title_lines, title_size_now, title_lh = fit_wrapped_lines(
            [title],
            title_width,
            height - 26,
            title_size,
            title_min,
            0.56,
            1.12,
        )
        return title_lines, title_size_now, title_lh, [], 0.0, 0.0
    while True:
        title_lines, title_size_now, title_lh = fit_wrapped_lines(
            [title],
            title_width,
            46,
            title_size,
            title_min,
            0.56,
            1.12,
        )
        gap = 10
        body_available = height - 34 - len(title_lines) * title_lh - gap
        body_lines, body_size_now, body_lh = fit_wrapped_lines(
            body,
            body_width,
            max(24, body_available),
            body_size,
            body_min,
            0.56,
            1.34,
        )
        total_h = 34 + len(title_lines) * title_lh + gap + len(body_lines) * body_lh
        if total_h <= height - 8:
            return title_lines, title_size_now, title_lh, body_lines, body_size_now, body_lh
        if body_size > body_min:
            body_size = round(body_size - 0.5, 2)
            continue
        if title_size > title_min:
            title_size = round(title_size - 0.5, 2)
            continue
        return title_lines, title_size_now, title_lh, body_lines, body_size_now, body_lh


def fit_centered_copy(
    title: str,
    desc: str,
    width: float,
    height: float,
) -> Tuple[List[str], float, float, List[str], float, float]:
    title_size = 15.0
    body_size = 12.5
    title_lines, title_size, title_lh = fit_wrapped_lines([title], width - 20, 38, title_size, 12.0, 0.56, 1.1)
    gap = 10
    body_available = height - 28 - len(title_lines) * title_lh - gap
    body_lines, body_size, body_lh = fit_wrapped_lines([desc], width - 20, body_available, body_size, 12.0, 0.56, 1.3)
    return title_lines, title_size, title_lh, body_lines, body_size, body_lh


def panel(lines: List[str], x: float, y: float, w: float, h: float, title: str, fill: str, accent: str) -> None:
    lines.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="24" ry="24" fill="{fill}" stroke="{LINE}" stroke-width="1.2"/>')
    lines.append(f'<rect x="{x + 18}" y="{y + 16}" width="7" height="28" rx="3.5" ry="3.5" fill="{accent}"/>')
    add_text(lines, x + 38, y + 37, title, "panel")


def card(
    lines: List[str],
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    body: Sequence[str],
    fill: str,
    accent: str,
    badge: str = "",
    max_chars: int = 34,
) -> None:
    lines.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="18" ry="18" fill="{fill}" stroke="{LINE}" stroke-width="1.25"/>')
    lines.append(f'<rect x="{x}" y="{y}" width="10" height="{h}" rx="18" ry="18" fill="{accent}"/>')
    title_x = x + 24
    if badge:
        lines.append(f'<circle cx="{x + 30}" cy="{y + 28}" r="13" fill="{accent}"/>')
        add_text(lines, x + 30, y + 33, badge, "badge", "middle")
        title_x = x + 54
    title_lines, title_size, title_lh, body_lines, body_size, body_lh = fit_card_copy(title, body, w - 24, h - 10, badge)
    if body_lines:
        title_y = y + 29
    else:
        title_block_h = len(title_lines) * title_lh
        title_y = y + (h - title_block_h) / 2 + 2
    add_multiline_sized(lines, title_x, title_y, title_lines, title_size, 700, INK, line_h=title_lh)
    if body_lines:
        body_y = title_y + (len(title_lines) - 1) * title_lh + 20
        add_multiline_sized(lines, x + 24, body_y, body_lines, body_size, 400, INK, line_h=body_lh)


def frame_box(lines: List[str], x: float, y: float, w: float, h: float, title: str, desc: str, fill: str) -> None:
    lines.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" ry="16" fill="{fill}" stroke="{LINE}" stroke-width="1.2"/>')
    title_lines, title_size, title_lh, body_lines, body_size, body_lh = fit_centered_copy(title, desc, w, h)
    add_multiline_sized(lines, x + w / 2, y + 26, title_lines, title_size, 700, INK, "middle", title_lh)
    desc_y = y + 26 + (len(title_lines) - 1) * title_lh + 18
    add_multiline_sized(lines, x + w / 2, desc_y, body_lines, body_size, 400, SUB, "middle", body_lh)


def arrow(lines: List[str], x1: float, y1: float, x2: float, y2: float, dashed: bool = False) -> None:
    dash = ' stroke-dasharray="8 6"' if dashed else ""
    lines.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{INK}" stroke-width="2.2"{dash} marker-end="url(#arrow)"/>')


def elbow(lines: List[str], points: Iterable[Tuple[float, float]], dashed: bool = False) -> None:
    dash = ' stroke-dasharray="8 6"' if dashed else ""
    path = " ".join(f"{x},{y}" for x, y in points)
    lines.append(f'<polyline points="{path}" fill="none" stroke="{INK}" stroke-width="2.2"{dash} marker-end="url(#arrow)"/>')


def diamond(lines: List[str], cx: float, cy: float, w: float, h: float, text: Sequence[str]) -> None:
    pts = [(cx, cy - h / 2), (cx + w / 2, cy), (cx, cy + h / 2), (cx - w / 2, cy)]
    poly = " ".join(f"{x},{y}" for x, y in pts)
    lines.append(f'<polygon points="{poly}" fill="{WHITE}" stroke="{LINE}" stroke-width="1.25"/>')
    wrapped, size, line_h = fit_wrapped_lines(text, w - 36, h - 24, 11.5, 9.0, 0.56, 1.28)
    start_y = cy - ((len(wrapped) - 1) * line_h) / 2
    add_multiline_sized(lines, cx, start_y, wrapped, size, 400, SUB, "middle", line_h)


def tag(lines: List[str], x: float, y: float, w: float, text: str) -> None:
    lines.append(f'<rect x="{x}" y="{y}" width="{w}" height="28" rx="14" ry="14" fill="{WHITE}" stroke="#DBE3E8" stroke-width="1"/>')
    add_text(lines, x + w / 2, y + 19, text, "meta", "middle")


def footer_note(lines: List[str], x: float, y: float, w: float, text: str) -> None:
    lines.append(f'<rect x="{x}" y="{y}" width="{w}" height="40" rx="15" ry="15" fill="{WHITE}" stroke="#D9E2E8" stroke-width="1"/>')
    add_text(lines, x + w / 2, y + 26, text, "meta", "middle")


def build_architecture() -> str:
    width, height = 1460, 500
    lines = svg_header(
        width,
        height,
        "Optbinlog System Architecture",
        "Shared metadata coordinates cache reuse, steady-state writing, and record-level verification.",
    )

    panel(lines, 36, 20, 220, 434, "Inputs and Constraints", MIST, NAVY)
    panel(lines, 282, 20, 900, 434, "Shared Core", WHITE, NAVY)
    panel(lines, 1208, 20, 216, 434, "Outputs and Verification", SAND, NAVY)

    card(lines, 58, 72, 176, 76, "Event Schema", [], BLUE, NAVY, "A", 22)
    card(lines, 58, 176, 176, 76, "Deployment Target", [], WHITE, NAVY, "B", 22)

    card(lines, 314, 58, 836, 82, "Shared Metadata File", ["Bitmaps, tag entries, and field descriptors stay in one shared layout."], BLUE, NAVY, "1", 52)
    card(lines, 314, 164, 394, 88, "Shared View Cache", ["Reuse the mapped view when identity stays unchanged."], GREEN, NAVY, "2", 28)
    card(lines, 756, 164, 394, 88, "Record Writer", ["Encode payload, build the header, and append the checksum."], SAND, NAVY, "3", 28)
    card(lines, 314, 282, 836, 78, "Consistency and Recovery", ["Single-creator setup and tail repair preserve stable behavior."], WHITE, NAVY, "4", 58)

    card(lines, 1228, 92, 176, 84, "Binary Log File", [], SAND, NAVY, "C", 22)
    card(lines, 1228, 210, 176, 88, "Reader and Check", [], BLUE, NAVY, "D", 22)

    arrow(lines, 234, 110, 314, 99)
    arrow(lines, 234, 214, 314, 321)
    arrow(lines, 522, 140, 522, 164)
    arrow(lines, 964, 140, 964, 164)
    arrow(lines, 522, 252, 522, 282)
    arrow(lines, 964, 252, 964, 282)
    arrow(lines, 1150, 208, 1228, 134)
    arrow(lines, 1316, 176, 1316, 210)
    elbow(lines, [(1316, 298), (1316, 408), (1150, 408)])
    return svg_footer(lines)


def build_structure() -> str:
    width, height = 1460, 510
    lines = svg_header(
        width,
        height,
        "Optbinlog Shared Metadata and Frame Layout",
        "The shared file explains structure; the frame carries the minimal evidence needed for validation and decoding.",
    )

    panel(lines, 36, 20, 274, 456, "Shared Metadata File", MIST, NAVY)
    panel(lines, 336, 20, 1088, 456, "Record Frame and Interpretation", SAND, NAVY)

    left_x = 56
    top_y = 68
    blocks = [
        ("File Header", [], 72, BLUE, "1"),
        ("Validity Bitmap", [], 72, WHITE, "2"),
        ("Tag Table", [], 78, GREEN, "3"),
        ("Field Table", [], 78, SAND, "4"),
        ("Reserved Area", [], 72, WHITE, "5"),
    ]
    cur_y = top_y
    for title, body, h, fill, badge in blocks:
        card(lines, left_x, cur_y, 234, h, title, body, fill, NAVY, badge, 21)
        cur_y += h + 8

    card(lines, 370, 64, 1020, 62, "Binary Record Frame", [], WHITE, NAVY, max_chars=76)

    frame_y = 140
    frame_x = 378
    parts = [
        ("Frame Header", "4 bytes", 152, BLUE),
        ("Timestamp", "8 bytes", 124, WHITE),
        ("Tag Identifier", "2 bytes", 132, WHITE),
        ("Field Count", "1 byte", 118, WHITE),
        ("Payload", "shared-schema encoding", 294, GREEN),
        ("Checksum", "4 bytes", 124, WHITE),
    ]
    cur_x = frame_x
    for title, desc, w, fill in parts:
        frame_box(lines, cur_x, frame_y, w, 72, title, desc, fill)
        cur_x += w + 10

    card(lines, 462, 250, 220, 86, "Header Semantics", ["Length, string mode, and checksum mode."], BLUE, NAVY, "A", 24)
    card(lines, 784, 250, 220, 86, "String Strategy", ["Fixed width or used length."], GREEN, NAVY, "B", 24)
    card(lines, 1106, 250, 246, 86, "Read Interpretation", ["Validate first, then decode by schema."], WHITE, NAVY, "C", 27)

    arrow(lines, 290, 250, 378, 176)
    arrow(lines, 730, 212, 572, 250)
    arrow(lines, 1028, 212, 894, 250)
    arrow(lines, 1318, 212, 1228, 250)
    return svg_footer(lines)


def build_flow() -> str:
    width, height = 1460, 510
    lines = svg_header(
        width,
        height,
        "Optbinlog Write, Read, and Recovery Flow",
        "The same shared event structure supports writing, reading, and damaged-tail recovery.",
    )

    panel(lines, 36, 20, 430, 458, "Stage 1: Shared View Setup", MIST, NAVY)
    panel(lines, 514, 20, 430, 458, "Stage 2: Steady-State Write", WHITE, NAVY)
    panel(lines, 992, 20, 430, 458, "Stage 3: Read and Recover", SAND, NAVY)

    card(lines, 62, 66, 378, 64, "Open the Shared Format File", [], BLUE, NAVY, max_chars=42)
    diamond(lines, 251, 166, 190, 68, ["File initialized", "identity unchanged"])
    card(lines, 62, 210, 378, 66, "Build or Reuse the Shared View", [], GREEN, NAVY, max_chars=42)
    card(lines, 62, 306, 378, 66, "Collect Structural Statistics", [], WHITE, NAVY, max_chars=42)

    card(lines, 540, 66, 378, 64, "Choose the String Strategy", [], GREEN, NAVY, max_chars=42)
    card(lines, 540, 162, 378, 66, "Encode Fields by Tag", [], BLUE, NAVY, max_chars=42)
    card(lines, 540, 258, 378, 66, "Build Header and Checksum", [], SAND, NAVY, max_chars=42)
    card(lines, 540, 354, 378, 66, "Flush the Complete Frame", [], WHITE, NAVY, max_chars=42)

    card(lines, 1018, 66, 378, 64, "Read the Frame Header", [], BLUE, NAVY, max_chars=42)
    card(lines, 1018, 152, 378, 66, "Read Payload and Checksum", [], GREEN, NAVY, max_chars=42)
    diamond(lines, 1207, 252, 206, 74, ["Header valid", "payload complete", "checksum matched"])
    card(lines, 1018, 324, 378, 66, "Decode by the Shared Structure", [], WHITE, NAVY, max_chars=42)
    card(lines, 1018, 414, 378, 50, "Repair a Damaged Tail", [], SAND, NAVY, max_chars=42)

    arrow(lines, 251, 130, 251, 136)
    arrow(lines, 251, 200, 251, 210)
    arrow(lines, 251, 276, 251, 306)
    elbow(lines, [(440, 339), (490, 339), (540, 98)])

    arrow(lines, 729, 130, 729, 162)
    arrow(lines, 729, 228, 729, 258)
    arrow(lines, 729, 324, 729, 354)
    elbow(lines, [(918, 387), (968, 387), (1018, 98)])

    arrow(lines, 1207, 130, 1207, 152)
    arrow(lines, 1207, 218, 1207, 225)
    arrow(lines, 1207, 289, 1207, 324)
    elbow(lines, [(1310, 252), (1396, 252), (1396, 439)])
    elbow(lines, [(1207, 464), (1207, 490), (729, 490), (729, 420)])
    return svg_footer(lines)


def write_file(path: str, content: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def main() -> None:
    ensure_dir(OUT_DIR)
    outputs = {
        "fig3_1_optbinlog_system_architecture.svg": build_architecture(),
        "fig3_2_optbinlog_metadata_frame_structure.svg": build_structure(),
        "fig3_3_optbinlog_write_read_recovery_flow.svg": build_flow(),
    }
    for name, content in outputs.items():
        path = os.path.join(OUT_DIR, name)
        write_file(path, content)
        print("saved", path)


if __name__ == "__main__":
    main()
