#!/usr/bin/env python3
"""Apply WHU undergraduate thesis formatting to a generated DOCX.

This script post-processes a DOCX by editing:
1) word/styles.xml (font/size/color/spacing/alignment for key styles)
2) word/document.xml (TOC field level and TOC heading text)
"""

from __future__ import annotations

import argparse
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET


W_NS = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
ET.register_namespace("w", W_NS)
ET.register_namespace("r", "http://schemas.openxmlformats.org/officeDocument/2006/relationships")


def wtag(name: str) -> str:
    return f"{{{W_NS}}}{name}"


def wval(name: str) -> str:
    return f"{{{W_NS}}}{name}"


def find_style(root: ET.Element, style_id: str) -> ET.Element | None:
    for style in root.findall(wtag("style")):
        if style.get(wval("styleId")) == style_id:
            return style
    return None


def ensure_child(parent: ET.Element, name: str) -> ET.Element:
    node = parent.find(wtag(name))
    if node is None:
        node = ET.SubElement(parent, wtag(name))
    return node


def clear_children(node: ET.Element) -> None:
    for child in list(node):
        node.remove(child)


def set_rpr(
    rpr: ET.Element,
    *,
    east_asia_font: str,
    size_half_points: int,
    bold: bool = False,
    italic: bool = False,
    ascii_font: str = "Times New Roman",
    color: str = "000000",
) -> None:
    clear_children(rpr)
    rfonts = ET.SubElement(rpr, wtag("rFonts"))
    rfonts.set(wval("ascii"), ascii_font)
    rfonts.set(wval("hAnsi"), ascii_font)
    rfonts.set(wval("cs"), ascii_font)
    rfonts.set(wval("eastAsia"), east_asia_font)
    if bold:
        ET.SubElement(rpr, wtag("b"))
        ET.SubElement(rpr, wtag("bCs"))
    if italic:
        ET.SubElement(rpr, wtag("i"))
        ET.SubElement(rpr, wtag("iCs"))
    color_node = ET.SubElement(rpr, wtag("color"))
    color_node.set(wval("val"), color)
    sz = ET.SubElement(rpr, wtag("sz"))
    sz.set(wval("val"), str(size_half_points))
    sz_cs = ET.SubElement(rpr, wtag("szCs"))
    sz_cs.set(wval("val"), str(size_half_points))


def set_ppr(
    ppr: ET.Element,
    *,
    align: str,
    line: int = 360,
    before: int = 0,
    after: int = 0,
    first_line: int | None = None,
    outline_level: int | None = None,
    keep_next: bool = False,
    keep_lines: bool = False,
) -> None:
    clear_children(ppr)
    if keep_next:
        ET.SubElement(ppr, wtag("keepNext"))
    if keep_lines:
        ET.SubElement(ppr, wtag("keepLines"))
    spacing = ET.SubElement(ppr, wtag("spacing"))
    spacing.set(wval("before"), str(before))
    spacing.set(wval("after"), str(after))
    spacing.set(wval("line"), str(line))
    spacing.set(wval("lineRule"), "auto")
    if first_line is not None:
        ind = ET.SubElement(ppr, wtag("ind"))
        ind.set(wval("firstLine"), str(first_line))
    jc = ET.SubElement(ppr, wtag("jc"))
    jc.set(wval("val"), align)
    if outline_level is not None:
        out = ET.SubElement(ppr, wtag("outlineLvl"))
        out.set(wval("val"), str(outline_level))


def ensure_paragraph_style(root: ET.Element, style_id: str, name: str, based_on: str = "Normal") -> ET.Element:
    style = find_style(root, style_id)
    if style is None:
        style = ET.SubElement(root, wtag("style"))
        style.set(wval("type"), "paragraph")
        style.set(wval("styleId"), style_id)
        name_node = ET.SubElement(style, wtag("name"))
        name_node.set(wval("val"), name)
        based = ET.SubElement(style, wtag("basedOn"))
        based.set(wval("val"), based_on)
    else:
        name_node = style.find(wtag("name"))
        if name_node is None:
            name_node = ET.SubElement(style, wtag("name"))
        name_node.set(wval("val"), name)
        based = style.find(wtag("basedOn"))
        if based is None:
            based = ET.SubElement(style, wtag("basedOn"))
        based.set(wval("val"), based_on)
    return style


def patch_doc_defaults(root: ET.Element) -> None:
    doc_defaults = ensure_child(root, "docDefaults")
    rpr_default = ensure_child(doc_defaults, "rPrDefault")
    rpr = ensure_child(rpr_default, "rPr")
    set_rpr(rpr, east_asia_font="宋体", size_half_points=24, bold=False, italic=False)

    ppr_default = ensure_child(doc_defaults, "pPrDefault")
    ppr = ensure_child(ppr_default, "pPr")
    set_ppr(ppr, align="both", line=360, before=0, after=0, first_line=420)


def patch_style_block(
    root: ET.Element,
    *,
    style_id: str,
    east_asia_font: str,
    size: int,
    align: str,
    bold: bool = False,
    italic: bool = False,
    line: int = 360,
    before: int = 0,
    after: int = 0,
    first_line: int | None = None,
    outline_level: int | None = None,
    keep_next: bool = False,
    keep_lines: bool = False,
) -> None:
    style = find_style(root, style_id)
    if style is None:
        return
    ppr = ensure_child(style, "pPr")
    rpr = ensure_child(style, "rPr")
    set_ppr(
        ppr,
        align=align,
        line=line,
        before=before,
        after=after,
        first_line=first_line,
        outline_level=outline_level,
        keep_next=keep_next,
        keep_lines=keep_lines,
    )
    set_rpr(
        rpr,
        east_asia_font=east_asia_font,
        size_half_points=size,
        bold=bold,
        italic=italic,
    )


def patch_styles_xml(path: Path) -> None:
    tree = ET.parse(path)
    root = tree.getroot()

    patch_doc_defaults(root)

    # Body: Songti, 小四, 1.5 line spacing, 两字符首行缩进（近似 420 twips）。
    for sid in ("Normal", "BodyText", "FirstParagraph", "Compact"):
        patch_style_block(
            root,
            style_id=sid,
            east_asia_font="宋体",
            size=24,
            align="both",
            bold=False,
            line=360,
            before=0,
            after=0,
            first_line=420,
        )

    # Heading mapping for this thesis document structure:
    # Heading1 -> title only; Heading2/3/4 -> chapter/section/subsection.
    patch_style_block(
        root,
        style_id="Heading1",
        east_asia_font="楷体",
        size=52,  # 一号（论文题名）
        align="center",
        bold=False,
        line=360,
        before=0,
        after=120,
        first_line=None,
        outline_level=0,
        keep_next=True,
        keep_lines=True,
    )
    patch_style_block(
        root,
        style_id="Heading2",
        east_asia_font="黑体",
        size=36,  # 小二（章标题）
        align="center",
        bold=True,
        line=360,
        before=120,
        after=120,
        first_line=None,
        outline_level=1,
        keep_next=True,
        keep_lines=True,
    )
    patch_style_block(
        root,
        style_id="Heading3",
        east_asia_font="黑体",
        size=28,  # 四号（节标题）
        align="left",
        bold=True,
        line=360,
        before=60,
        after=60,
        first_line=None,
        outline_level=2,
        keep_next=True,
        keep_lines=True,
    )
    patch_style_block(
        root,
        style_id="Heading4",
        east_asia_font="黑体",
        size=24,  # 小四（条标题）
        align="left",
        bold=True,
        line=360,
        before=20,
        after=20,
        first_line=None,
        outline_level=3,
        keep_next=True,
        keep_lines=True,
    )

    # TOC heading.
    patch_style_block(
        root,
        style_id="TOCHeading",
        east_asia_font="黑体",
        size=36,
        align="center",
        bold=True,
        line=360,
        before=0,
        after=120,
        first_line=None,
        outline_level=9,
    )

    # Figure/table captions: 宋体五号（10.5pt=21 half-points）.
    for sid in ("Caption", "ImageCaption", "TableCaption"):
        patch_style_block(
            root,
            style_id=sid,
            east_asia_font="宋体",
            size=21,
            align="center",
            bold=True,
            line=360,
            before=0,
            after=0,
            first_line=None,
        )

    # Ensure TOC levels exist with expected fonts/sizes.
    toc_specs = {
        "TOC1": ("TOC 1", True),
        "TOC2": ("TOC 2", False),
        "TOC3": ("TOC 3", False),
    }
    for sid, (name, bold) in toc_specs.items():
        style = ensure_paragraph_style(root, sid, name, based_on="Normal")
        ppr = ensure_child(style, "pPr")
        rpr = ensure_child(style, "rPr")
        set_ppr(ppr, align="left", line=360, before=0, after=0, first_line=None)
        set_rpr(rpr, east_asia_font="宋体", size_half_points=24, bold=bold)

    # Hyperlinks should stay black in printed thesis.
    hyperlink = find_style(root, "Hyperlink")
    if hyperlink is not None:
        rpr = ensure_child(hyperlink, "rPr")
        set_rpr(rpr, east_asia_font="宋体", size_half_points=24, bold=False, color="000000")

    # Section number run style should also be black (for numbered headings).
    section_number = find_style(root, "SectionNumber")
    if section_number is not None:
        rpr = ensure_child(section_number, "rPr")
        set_rpr(rpr, east_asia_font="黑体", size_half_points=24, bold=False, color="000000")

    # Character heading styles can still leak theme colors; normalize them too.
    char_heading_specs = {
        "Heading1Char": 52,
        "Heading2Char": 36,
        "Heading3Char": 28,
        "Heading4Char": 24,
    }
    for sid, size in char_heading_specs.items():
        style = find_style(root, sid)
        if style is None:
            continue
        rpr = ensure_child(style, "rPr")
        set_rpr(rpr, east_asia_font="黑体", size_half_points=size, bold=True, color="000000")

    # Remove remaining theme color leakage from all styles.
    for color in root.findall(f".//{wtag('style')}//{wtag('rPr')}//{wtag('color')}"):
        color.set(wval("val"), "000000")
        for k in (wval("themeColor"), wval("themeTint"), wval("themeShade")):
            if k in color.attrib:
                del color.attrib[k]

    tree.write(path, encoding="utf-8", xml_declaration=True)


def patch_document_xml(path: Path) -> None:
    tree = ET.parse(path)
    root = tree.getroot()

    # TOC: map to Heading2-Heading3 (chapter/section), only two levels.
    for instr in root.findall(f".//{wtag('instrText')}"):
        if instr.text and "TOC " in instr.text:
            instr.text = 'TOC \\o "2-3" \\h \\z \\u'

    # Make sure TOC title text is Chinese "目录".
    # Only touch the paragraph with style TOCHeading.
    for p in root.findall(f".//{wtag('p')}"):
        ppr = p.find(wtag("pPr"))
        if ppr is None:
            continue
        pstyle = ppr.find(wtag("pStyle"))
        if pstyle is None or pstyle.get(wval("val")) != "TOCHeading":
            continue
        texts = p.findall(f".//{wtag('t')}")
        if texts:
            texts[0].text = "目录"
            for extra in texts[1:]:
                extra.text = ""

    body_styles = {"Normal", "BodyText", "FirstParagraph", "Compact"}
    heading_styles = {"Heading2", "Heading3", "Heading4"}

    def para_text(p: ET.Element) -> str:
        return "".join((t.text or "") for t in p.findall(f".//{wtag('t')}")).strip()

    def set_para_text(p: ET.Element, text: str) -> None:
        for child in list(p):
            if child.tag == wtag("r"):
                p.remove(child)
        r = ET.SubElement(p, wtag("r"))
        t = ET.SubElement(r, wtag("t"))
        if text.startswith(" ") or text.endswith(" "):
            t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        t.text = text

    def parse_heading_number(text: str) -> tuple[list[int], str] | None:
        m = re.match(r"^(\d+(?:\.\d+){0,2})\s+(.+?)\s*$", text)
        if not m:
            return None
        nums = [int(x) for x in m.group(1).split(".")]
        return nums, m.group(2)

    def set_para_style_id(ppr: ET.Element, style_id: str) -> None:
        pstyle = ensure_child(ppr, "pStyle")
        pstyle.set(wval("val"), style_id)

    def set_para_ppr(
        ppr: ET.Element,
        *,
        align: str,
        before: int,
        after: int,
        first_line: int | None,
    ) -> None:
        spacing = ensure_child(ppr, "spacing")
        spacing.set(wval("before"), str(before))
        spacing.set(wval("after"), str(after))
        spacing.set(wval("line"), "360")
        spacing.set(wval("lineRule"), "auto")

        if first_line is None:
            ind = ppr.find(wtag("ind"))
            if ind is not None:
                ppr.remove(ind)
        else:
            ind = ensure_child(ppr, "ind")
            ind.set(wval("firstLine"), str(first_line))

        jc = ensure_child(ppr, "jc")
        jc.set(wval("val"), align)

    def strip_theme_color(rpr: ET.Element) -> None:
        color = ensure_child(rpr, "color")
        color.set(wval("val"), "000000")
        for k in (wval("themeColor"), wval("themeTint"), wval("themeShade")):
            if k in color.attrib:
                del color.attrib[k]

    def set_run_font(rpr: ET.Element, east_asia: str, size: int) -> None:
        rfonts = ensure_child(rpr, "rFonts")
        rfonts.set(wval("ascii"), "Times New Roman")
        rfonts.set(wval("hAnsi"), "Times New Roman")
        rfonts.set(wval("cs"), "Times New Roman")
        rfonts.set(wval("eastAsia"), east_asia)

        sz = ensure_child(rpr, "sz")
        sz.set(wval("val"), str(size))
        sz_cs = ensure_child(rpr, "szCs")
        sz_cs.set(wval("val"), str(size))
        strip_theme_color(rpr)

    body = root.find(wtag("body"))

    # Pass 1: fix heading hierarchy by numeric depth (e.g. 5.2.1 must be Heading4).
    if body is not None:
        for p in body.findall(wtag("p")):
            ppr = ensure_child(p, "pPr")
            pstyle = ppr.find(wtag("pStyle"))
            sid = pstyle.get(wval("val")) if pstyle is not None else "Normal"
            if sid not in heading_styles:
                continue
            text = para_text(p)
            parsed = parse_heading_number(text)
            if not parsed:
                continue
            nums, _ = parsed
            target = {1: "Heading2", 2: "Heading3", 3: "Heading4"}.get(len(nums))
            if target and sid != target:
                set_para_style_id(ppr, target)

    # Pass 2: rebuild heading numbers to remove duplicates and misalignment.
    if body is not None:
        chapter = 0
        section = 0
        sub = 0
        for p in body.findall(wtag("p")):
            ppr = ensure_child(p, "pPr")
            pstyle = ppr.find(wtag("pStyle"))
            sid = pstyle.get(wval("val")) if pstyle is not None else "Normal"
            if sid not in heading_styles:
                continue
            text = para_text(p)
            parsed = parse_heading_number(text)
            if not parsed:
                continue
            _, title = parsed

            if sid == "Heading2":
                chapter += 1
                section = 0
                sub = 0
                set_para_text(p, f"{chapter} {title}")
            elif sid == "Heading3":
                if chapter == 0:
                    continue
                section += 1
                sub = 0
                set_para_text(p, f"{chapter}.{section} {title}")
            elif sid == "Heading4":
                if chapter == 0 or section == 0:
                    continue
                sub += 1
                set_para_text(p, f"{chapter}.{section}.{sub} {title}")

    for p in root.findall(f".//{wtag('p')}"):
        ppr = ensure_child(p, "pPr")
        pstyle = ppr.find(wtag("pStyle"))
        sid = pstyle.get(wval("val")) if pstyle is not None else "Normal"
        paragraph_text = para_text(p)

        if sid == "Heading1":
            set_para_ppr(ppr, align="center", before=0, after=120, first_line=None)
            target_font, target_size = "楷体", 52
        elif sid == "Heading2":
            set_para_ppr(ppr, align="center", before=120, after=120, first_line=None)
            target_font, target_size = "黑体", 36
        elif sid == "Heading3":
            set_para_ppr(ppr, align="left", before=60, after=60, first_line=None)
            target_font, target_size = "黑体", 28
        elif sid == "Heading4":
            set_para_ppr(ppr, align="left", before=40, after=40, first_line=None)
            target_font, target_size = "黑体", 24
        elif sid == "TOCHeading":
            set_para_ppr(ppr, align="center", before=0, after=120, first_line=None)
            target_font, target_size = "黑体", 36
        elif sid in {"TOC1", "TOC2", "TOC3"}:
            set_para_ppr(ppr, align="left", before=0, after=0, first_line=None)
            target_font, target_size = "宋体", 24
        elif sid in body_styles:
            set_para_ppr(ppr, align="both", before=0, after=0, first_line=420)
            target_font, target_size = "宋体", 24
        else:
            target_font, target_size = "宋体", 24

        # Fine-grained paragraph alignment overrides for template compliance.
        if paragraph_text.startswith("关键词：") or paragraph_text.startswith("Keywords:"):
            set_para_ppr(ppr, align="left", before=0, after=0, first_line=None)
        if "作者签名：" in paragraph_text or paragraph_text.startswith("日期："):
            set_para_ppr(ppr, align="center", before=0, after=0, first_line=None)
        if paragraph_text.startswith("作者：") and "单位：" in paragraph_text:
            set_para_ppr(ppr, align="center", before=0, after=0, first_line=None)

        for r in p.findall(wtag("r")):
            rpr = ensure_child(r, "rPr")
            set_run_font(rpr, target_font, target_size)

    # Cover-section centered layout (between "封面信息（按模板填写）" and next Heading2).
    if body is not None:
        in_cover = False
        for p in body.findall(wtag("p")):
            ppr = ensure_child(p, "pPr")
            ps = ppr.find(wtag("pStyle"))
            sid = ps.get(wval("val")) if ps is not None else "Normal"
            text = "".join((t.text or "") for t in p.findall(f".//{wtag('t')}")).strip()
            if sid == "Heading2":
                if text == "封面信息（按模板填写）":
                    in_cover = True
                    continue
                if in_cover:
                    in_cover = False
            if in_cover and text:
                set_para_ppr(ppr, align="center", before=0, after=0, first_line=None)

    # Move TOC block to after abstracts and before chapter 1 ("1 绪论").
    if body is not None:
        toc_sdt = None
        for child in list(body):
            if child.tag != wtag("sdt"):
                continue
            if child.find(f".//{wtag('instrText')}") is not None:
                toc_sdt = child
                break
        chapter1_p = None
        for p in body.findall(wtag("p")):
            text = "".join((t.text or "") for t in p.findall(f".//{wtag('t')}")).strip()
            if re.match(r"^1\s*绪论", text):
                chapter1_p = p
                break
        if toc_sdt is not None and chapter1_p is not None:
            body.remove(toc_sdt)
            insert_idx = list(body).index(chapter1_p)
            body.insert(insert_idx, toc_sdt)

    # Table beautification: unified borders, paddings, header shading, and compact cell text.
    if body is not None:
        for tbl in body.findall(f".//{wtag('tbl')}"):
            tbl_pr = ensure_child(tbl, "tblPr")

            tbl_style = ensure_child(tbl_pr, "tblStyle")
            tbl_style.set(wval("val"), "TableGrid")

            tbl_w = ensure_child(tbl_pr, "tblW")
            tbl_w.set(wval("type"), "pct")
            tbl_w.set(wval("w"), "5000")

            tbl_layout = ensure_child(tbl_pr, "tblLayout")
            tbl_layout.set(wval("type"), "fixed")

            tbl_jc = ensure_child(tbl_pr, "jc")
            tbl_jc.set(wval("val"), "center")

            borders = ensure_child(tbl_pr, "tblBorders")
            for side in ("top", "left", "bottom", "right", "insideH", "insideV"):
                side_node = ensure_child(borders, side)
                side_node.set(wval("val"), "single")
                side_node.set(wval("sz"), "8")
                side_node.set(wval("space"), "0")
                side_node.set(wval("color"), "000000")

            cell_mar = ensure_child(tbl_pr, "tblCellMar")
            for side, val in (("top", "80"), ("left", "100"), ("bottom", "80"), ("right", "100")):
                side_node = ensure_child(cell_mar, side)
                side_node.set(wval("w"), val)
                side_node.set(wval("type"), "dxa")

            rows = tbl.findall(wtag("tr"))
            for ridx, tr in enumerate(rows):
                is_header = ridx == 0
                for cidx, tc in enumerate(tr.findall(wtag("tc"))):
                    tc_pr = ensure_child(tc, "tcPr")
                    v_align = ensure_child(tc_pr, "vAlign")
                    v_align.set(wval("val"), "center")

                    shd = tc_pr.find(wtag("shd"))
                    if is_header:
                        if shd is None:
                            shd = ET.SubElement(tc_pr, wtag("shd"))
                        shd.set(wval("val"), "clear")
                        shd.set(wval("color"), "auto")
                        shd.set(wval("fill"), "F2F2F2")
                    elif shd is not None:
                        tc_pr.remove(shd)

                    for p in tc.findall(wtag("p")):
                        ppr = ensure_child(p, "pPr")
                        spacing = ensure_child(ppr, "spacing")
                        spacing.set(wval("before"), "0")
                        spacing.set(wval("after"), "0")
                        spacing.set(wval("line"), "300")
                        spacing.set(wval("lineRule"), "auto")

                        ind = ppr.find(wtag("ind"))
                        if ind is not None:
                            ppr.remove(ind)

                        jc = ensure_child(ppr, "jc")
                        # Header row centered; body row only first column ("category") centered.
                        if is_header:
                            jc.set(wval("val"), "center")
                        else:
                            jc.set(wval("val"), "center" if cidx == 0 else "left")

                        for r in p.findall(wtag("r")):
                            rpr = ensure_child(r, "rPr")
                            set_run_font(rpr, "宋体", 21)
                            if is_header:
                                ensure_child(rpr, "b")
                                ensure_child(rpr, "bCs")
                            else:
                                b = rpr.find(wtag("b"))
                                if b is not None:
                                    rpr.remove(b)
                                bcs = rpr.find(wtag("bCs"))
                                if bcs is not None:
                                    rpr.remove(bcs)

    tree.write(path, encoding="utf-8", xml_declaration=True)


def patch_document_rels_xml(path: Path) -> None:
    if not path.exists():
        return

    tree = ET.parse(path)
    root = tree.getroot()

    # Prefer PNG targets for compatibility (some viewers fail to render SVG in docx).
    image_rels = [r for r in root.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship") if "image" in (r.get("Type") or "")]
    targets = {r.get("Target") for r in image_rels}

    for rel in image_rels:
        target = rel.get("Target") or ""
        if not target.lower().endswith(".svg"):
            continue
        m = re.search(r"rId(\d+)\.svg$", target)
        if not m:
            continue
        rid_num = int(m.group(1))
        fallback = f"media/rId{rid_num + 3}.png"
        if fallback in targets:
            rel.set("Target", fallback)

    tree.write(path, encoding="utf-8", xml_declaration=True)


def patch_settings_xml(path: Path) -> None:
    if not path.exists():
        return
    tree = ET.parse(path)
    root = tree.getroot()
    update = root.find(wtag("updateFields"))
    if update is None:
        update = ET.SubElement(root, wtag("updateFields"))
    update.set(wval("val"), "true")
    tree.write(path, encoding="utf-8", xml_declaration=True)


def repack_docx(folder: Path, out_docx: Path) -> None:
    with zipfile.ZipFile(out_docx, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(folder.rglob("*")):
            if f.is_file():
                zf.write(f, f.relative_to(folder).as_posix())


def process_docx(input_docx: Path, output_docx: Path) -> None:
    if not input_docx.exists():
        raise FileNotFoundError(f"Input DOCX not found: {input_docx}")

    with tempfile.TemporaryDirectory(prefix="whu_docx_fmt_") as tmpdir:
        tmp = Path(tmpdir)
        with zipfile.ZipFile(input_docx, "r") as zf:
            zf.extractall(tmp)

        styles = tmp / "word" / "styles.xml"
        document = tmp / "word" / "document.xml"
        rels = tmp / "word" / "_rels" / "document.xml.rels"
        settings = tmp / "word" / "settings.xml"
        if not styles.exists() or not document.exists():
            raise FileNotFoundError("DOCX missing word/styles.xml or word/document.xml")

        patch_styles_xml(styles)
        patch_document_xml(document)
        patch_document_rels_xml(rels)
        patch_settings_xml(settings)
        repack_docx(tmp, output_docx)


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply WHU thesis formatting to DOCX")
    parser.add_argument("input_docx", type=Path, help="Input DOCX path")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output DOCX path (default: overwrite input)",
    )
    args = parser.parse_args()

    input_docx = args.input_docx.resolve()
    output_docx = args.output.resolve() if args.output else input_docx

    if output_docx == input_docx:
        backup = input_docx.with_suffix(".pre_whu_format.bak.docx")
        shutil.copy2(input_docx, backup)

    process_docx(input_docx, output_docx)


if __name__ == "__main__":
    main()
