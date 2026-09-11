from __future__ import annotations

import contextlib
import copy
import re
from collections.abc import Callable
from pathlib import Path

from docx.document import Document as DocumentT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.oxml.xmlchemy import BaseOxmlElement
from docx.shared import Inches, Pt, RGBColor
from docx.table import Table, _Cell, _Row
from docx.text.paragraph import Paragraph

FONT = "Arial Narrow"
CENTER = WD_ALIGN_PARAGRAPH.CENTER
# Fond des en-têtes et lignes « Total » des tableaux de données (gris).
HEADER_FILL = "BFBFBF"
# Bordures « filets horizontaux » des tableaux de données : contour haut/bas
BORDER_THICK = 12
BORDER_THIN = 2
_TBLPR_AFTER_BORDERS = ("w:shd", "w:tblLayout", "w:tblCellMar", "w:tblLook")


def norm(s: str) -> str:
    """Normalise un libellé : apostrophe droite, espaces/retours compactés.

    Returns:
        La chaîne normalisée en minuscules, avec les espaces et retours compactés.
    """
    return " ".join(s.replace("’", "'").split()).lower()  # noqa: RUF001


def set_cell(
    cell: _Cell,
    text: str,
    *,
    bold: bool = False,
    align: WD_ALIGN_PARAGRAPH | None = None,
    color: str | None = None,
    size: int = 10,
) -> None:
    """Set text and formatting for a table cell.

    Args:
        cell: The docx table cell to update.
        text: The text content to insert into the cell.
        bold: Whether the text should be bold.
        align: Optional paragraph alignment for the cell.
        color: Optional RGB color string for the text.
        size: Font size in points (10 par défaut).
    """
    cell.text = ""
    p = cell.paragraphs[0]
    if align is not None:
        p.alignment = align
    run = p.add_run(str(text))
    run.bold = bold
    run.font.name = FONT
    run.font.size = Pt(size)
    if color:
        run.font.color.rgb = RGBColor.from_string(color)


def set_cell_shading(cell: _Cell, fill: str = HEADER_FILL) -> None:
    """Applique une couleur de fond (hex sans ``#``) à une cellule de tableau.

    Args:
        cell: La cellule à colorer.
        fill: La couleur de remplissage hexadécimale (``BFBFBF`` par défaut).
    """
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = tc_pr.find(qn("w:shd"))
    if shd is None:
        shd = OxmlElement("w:shd")
        tc_pr.append(shd)
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), fill)


def _border_el(side: str, sz: int) -> BaseOxmlElement:
    """Construit un élément de bordure ``w:<side>`` (``none`` si ``sz <= 0``).

    Returns:
        L'élément XML de bordure prêt à insérer.
    """
    el = OxmlElement(f"w:{side}")
    if sz <= 0:
        el.set(qn("w:val"), "none")
        el.set(qn("w:sz"), "0")
    else:
        el.set(qn("w:val"), "single")
        el.set(qn("w:sz"), str(sz))
    el.set(qn("w:space"), "0")
    el.set(qn("w:color"), "auto")
    return el


def set_table_borders(
    table: Table,
    *,
    thick: int = BORDER_THICK,
    thin: int = BORDER_THIN,
) -> None:
    """Applique le style « filets horizontaux » aux bordures d'un tableau.

    Args:
        table: Le tableau à border.
        thick: Épaisseur (douzièmes de pt) du contour haut/bas.
        thin: Épaisseur (douzièmes de pt) de la grille interne (0 = aucune).
    """
    tbl_pr = table._tbl.tblPr
    for old in tbl_pr.findall(qn("w:tblBorders")):
        tbl_pr.remove(old)
    borders = OxmlElement("w:tblBorders")
    for side, sz in (
        ("top", thick),
        ("left", 0),
        ("bottom", thick),
        ("right", 0),
        ("insideH", thin),
        ("insideV", thin),
    ):
        borders.append(_border_el(side, sz))

    for tag in _TBLPR_AFTER_BORDERS:
        ref = tbl_pr.find(qn(tag))
        if ref is not None:
            ref.addprevious(borders)
            break
    else:
        tbl_pr.append(borders)


def mark_header_row(row: _Row) -> None:
    """Marque une ligne comme en-tête répétée sur chaque page (``w:tblHeader``).

    Sans effet si le tableau ne s'étend que sur une seule page ; répète la
    ligne en haut de chaque page suivante si Word doit le scinder.
    """
    tr_pr = row._tr.get_or_add_trPr()
    if tr_pr.find(qn("w:tblHeader")) is None:
        tr_pr.append(OxmlElement("w:tblHeader"))


def set_cell_border_bottom(cell: _Cell, sz: int = BORDER_THICK) -> None:
    """Pose un filet épais sous une cellule (séparateur sous l'en-tête).

    Args:
        cell: La cellule à souligner.
        sz: Épaisseur du filet (douzièmes de pt).
    """
    tc_pr = cell._tc.get_or_add_tcPr()
    borders = tc_pr.find(qn("w:tcBorders"))
    if borders is None:
        borders = OxmlElement("w:tcBorders")
        tc_pr.append(borders)
    bottom = borders.find(qn("w:bottom"))
    if bottom is not None:
        borders.remove(bottom)
    borders.append(_border_el("bottom", sz))


class Cursor:
    """Insère des éléments XML à la suite d'un élément d'ancrage."""

    def __init__(self, anchor_element: BaseOxmlElement) -> None:
        self._el = anchor_element

    def add(self, new_element: BaseOxmlElement) -> None:
        """Insert a new XML element after the current anchor and move the cursor.

        Args:
            new_element: The XML element to insert after the current cursor position.
        """
        self._el.addnext(new_element)
        self._el = new_element


def para(
    doc: DocumentT,
    text: str = "",
    *,
    bold: bool = False,
    size: int = 10,
    italic: bool = False,
    color: str | None = None,
    align: WD_ALIGN_PARAGRAPH | None = None,
) -> Paragraph:
    """Add a paragraph to the document with optional formatting.

    Args:
        doc: Document to add the paragraph to.
        text: Text content for the paragraph.
        bold: Whether the text should be bold.
        size: Font size in points.
        italic: Whether the text should be italic.
        color: Optional RGB color string for the text.
        align: Optional paragraph alignment.

    Returns:
        The created paragraph.
    """
    p = doc.add_paragraph()
    if align is not None:
        p.alignment = align
    if text:
        run = p.add_run(text)
        run.bold = bold
        run.italic = italic
        run.font.name = FONT
        run.font.size = Pt(size)
        if color:
            run.font.color.rgb = RGBColor.from_string(color)
    return p


def bullet(doc: DocumentT, text: str) -> Paragraph:
    """Puce manuelle (caractère « • », pas un style de paragraphe).

    Volontairement pas de ``style="List Bullet"`` : un style de ce nom peut
    exister dans le template sans être un vrai style à puce (numérotation
    automatique héritée), rendu alors « 1. », « 2. »… au lieu de puces.
    ``text`` peut marquer du texte en gras avec ``**...**`` (même syntaxe
    que ``fill_shape_lines``, cf. ``_split_bold`` — jamais rendu littéralement).

    Returns:
        The created paragraph.
    """
    p = doc.add_paragraph()
    p.paragraph_format.left_indent = Inches(0.25)
    prefix = p.add_run("•  ")
    prefix.font.name = FONT
    prefix.font.size = Pt(10)
    for seg_text, bold in _split_bold(text):
        run = p.add_run(seg_text)
        run.font.name = FONT
        run.font.size = Pt(10)
        run.bold = bold
    return p


def centered_line(doc: DocumentT, text: str) -> Paragraph:
    """Paragraphe simple centré, sans puce (ex. blocs de contact).

    Returns:
        The created paragraph.
    """
    p = doc.add_paragraph()
    p.alignment = CENTER
    run = p.add_run(text)
    run.font.name = FONT
    run.font.size = Pt(10)
    return p


def table(
    doc: DocumentT,
    headers: list[str],
    rows: list[list],
    *,
    table_title: str | None = None,
    table_title_fill: str = HEADER_FILL,
    table_title_font_color: str = "FFFFFF",
    total_fill: str = HEADER_FILL,
    total_font_color: str | None = None,
) -> Table:
    """Create a table in the document with the given headers and rows.

    Args:
        doc: The document where the table will be inserted.
        headers: A list of header titles for the table columns.
        rows: A list of rows, each row being a list of cell values.
        table_title: Titre du tableau (ex. « Tableau 1. … »), sur une ligne
            fusionnée au-dessus de l'en-tête si fourni — sinon pas de ligne
            de titre.
        table_title_fill: Couleur de fond de la ligne de titre.
        table_title_font_color: Couleur du texte de la ligne de titre.
        total_fill: Couleur de fond de la ligne « Total » (gris par défaut).
        total_font_color: Couleur du texte de la ligne « Total » (héritée si
            ``None`` — ex. blanc sur un fond rouge).

    Returns:
        The created table object.
    """
    n_title_rows = 1 if table_title else 0
    t = doc.add_table(rows=1 + n_title_rows, cols=len(headers))
    with contextlib.suppress(KeyError):
        t.style = "Table Grid"

    # Bordures « filets horizontaux » (contour haut/bas, grille interne fine).
    set_table_borders(t)

    header_row = 0
    if table_title:
        merged = t.cell(0, 0).merge(t.cell(0, len(headers) - 1))
        set_cell(merged, table_title, bold=True, align=CENTER, color=table_title_font_color)
        set_cell_shading(merged, table_title_fill)
        mark_header_row(t.rows[0])
        header_row = 1

    # Répète la/les ligne(s) d'en-tête sur chaque page si le tableau est scindé.
    mark_header_row(t.rows[header_row])

    # En-tête : texte gras sur fond gris + filet épais en dessous.
    for j, h in enumerate(headers):
        set_cell(t.cell(header_row, j), h, bold=True, align=CENTER)
        set_cell_shading(t.cell(header_row, j))
        set_cell_border_bottom(t.cell(header_row, j))

    for r in rows:
        cells = t.add_row().cells
        is_total = str(r[0]).strip().lower() == "total"
        for j, cell in enumerate(cells):
            set_cell(
                cell,
                r[j],
                bold=is_total,
                align=CENTER if (j or is_total) else None,
                color=total_font_color if is_total else None,
            )
            if is_total:
                set_cell_shading(cell, total_fill)

    # Filet de clôture : bottom explicite sur la dernière ligne — Word ne rend
    if len(t.rows) > 1:
        for cell in t.rows[-1].cells:
            set_cell_border_bottom(cell)
    return t


def province_zone_table(
    doc: DocumentT,
    group_headers: list[tuple[str, int]],
    sub_headers: list[str],
    rows: list[dict],
    *,
    first_col_header: str = "Province / Zone de santé",
    province_fill: str = "D9E8F5",
    table_title: str | None = None,
    header_fill: str = HEADER_FILL,
    header_font_color: str | None = None,
) -> Table:
    """Tableau à en-tête groupé (2 lignes) pour le Tableau 2 (par ZS).

    Args:
        group_headers: ``[(titre, nb_colonnes), ...]`` pour la 2ᵉ ligne
            d'en-tête (ex. ``[("Nombre cumulatif", 3), ("Situation du jour
            (24h)", 4)]``), après la 1ʳᵉ colonne (fusionnée verticalement).
        sub_headers: Les libellés de colonne sous chaque groupe (à plat,
            même nombre total que la somme des tailles de ``group_headers``).
        rows: Une ligne par entrée, ``{"label": str, "is_province": bool,
            "values": list}`` — ``values`` a la même longueur que
            ``sub_headers``. Une ligne « province » (``is_province=True``)
            est mise en gras et surlignée en bleu clair.
        province_fill: Couleur de fond des lignes « province ».
        table_title: Titre du tableau (ex. « Tableau 2. … »), sur une ligne
            fusionnée au-dessus des 2 lignes d'en-tête si fourni.
        header_fill: Couleur de fond des 2 lignes d'en-tête (groupe +
            sous-titres, non gras).
        header_font_color: Couleur du texte des 2 lignes d'en-tête.

    Returns:
        The created table object.
    """
    n_cols = 1 + len(sub_headers)
    n_title_rows = 1 if table_title else 0
    t = doc.add_table(rows=2 + n_title_rows, cols=n_cols)
    with contextlib.suppress(KeyError):
        t.style = "Table Grid"
    set_table_borders(t)

    header_row = 0
    if table_title:
        merged_title = t.cell(0, 0).merge(t.cell(0, n_cols - 1))
        set_cell(merged_title, table_title, bold=True, align=CENTER, color="FFFFFF")
        set_cell_shading(merged_title, header_fill)
        mark_header_row(t.rows[0])
        header_row = 1

    # Répète les 2 lignes d'en-tête (groupe + sous-titres) sur chaque page.
    mark_header_row(t.rows[header_row])
    mark_header_row(t.rows[header_row + 1])

    # Ligne 1 : 1re colonne fusionnée verticalement + titres de groupe fusionnés horizontalement.
    top_left = t.cell(header_row, 0).merge(t.cell(header_row + 1, 0))
    set_cell(top_left, first_col_header, bold=False, align=CENTER, color=header_font_color)
    set_cell_shading(top_left, header_fill)

    col = 1
    for grp_title, span in group_headers:
        first = t.cell(header_row, col)
        merged = first
        for k in range(1, span):
            merged = merged.merge(t.cell(header_row, col + k))
        set_cell(merged, grp_title, bold=False, align=CENTER, color=header_font_color)
        set_cell_shading(merged, header_fill)
        col += span

    # Ligne 2 : libellés de colonne individuels.
    for j, h in enumerate(sub_headers, start=1):
        set_cell(t.cell(header_row + 1, j), h, bold=False, align=CENTER, size=9, color=header_font_color)
        set_cell_shading(t.cell(header_row + 1, j), header_fill)
        set_cell_border_bottom(t.cell(header_row + 1, j))
    set_cell_border_bottom(top_left)

    for r in rows:
        cells = t.add_row().cells
        is_province = bool(r.get("is_province"))
        is_total = str(r.get("label", "")).strip().lower() == "total"
        set_cell(cells[0], r["label"], bold=is_province or is_total)
        if is_province or is_total:
            set_cell_shading(cells[0], province_fill)
        for j, val in enumerate(r["values"], start=1):
            set_cell(cells[j], val, bold=is_province or is_total, align=CENTER)
            if is_province or is_total:
                set_cell_shading(cells[j], province_fill)

    if len(t.rows) > 2:
        for cell in t.rows[-1].cells:
            set_cell_border_bottom(cell)
    return t


def marker_paragraph(doc: DocumentT, token: str) -> Paragraph | None:
    """Retourne le premier paragraphe dont le texte commence par ``token``.

    Returns:
        Paragraph | None: Le paragraphe marqueur, ou ``None`` s'il est absent.
    """
    for p in doc.paragraphs:
        if p.text.strip().startswith(token):
            return p
    return None


def replace_marker(doc: DocumentT, token: str, fill: Callable[[Cursor], None]) -> None:
    """Insère le contenu produit par ``fill(cursor)`` puis supprime le marqueur."""
    marker = marker_paragraph(doc, token)
    if marker is None:
        return
    cur = Cursor(marker._p)
    fill(cur)
    marker._p.getparent().remove(marker._p)


def find_table(doc: DocumentT, predicate: Callable[[Table], bool]) -> Table | None:
    """Retourne la première table satisfaisant ``predicate``.

    Returns:
        Table | None: La table trouvée, ou ``None`` si aucune ne correspond.
    """
    for t in doc.tables:
        try:
            if predicate(t):
                return t
        except Exception:
            continue
    return None


_MARKER_RE = re.compile(r"\[\[[A-Z_0-9]+\]\]")


def normalize_markers(doc: DocumentT) -> None:
    """Fusionne les marqueurs ``[[...]]`` éclatés sur plusieurs runs/``w:t``.

    Word (autocorrection, copier-coller, vérification orthographique) coupe
    parfois un marqueur fraîchement tapé en plusieurs runs (ex.
    « [[KPI_CUMUL_DE » + « CES]] ») — invisible pour ``set_inline_marker``/
    ``_marker_run``, qui n'inspectent qu'un ``w:t`` à la fois. À appeler une
    fois en tête de ``render()``, avant tout remplissage de marqueur.
    """
    for p in doc.element.body.iter(qn("w:p")):
        chunks = [
            (t, t.text)
            for r in p.findall(qn("w:r"))
            if (t := r.find(qn("w:t"))) is not None and t.text
        ]
        if len(chunks) < 2:
            continue
        full = "".join(txt for _, txt in chunks)
        if not _MARKER_RE.search(full):
            continue
        bounds = []
        pos = 0
        for _, txt in chunks:
            bounds.append((pos, pos + len(txt)))
            pos += len(txt)
        for m in _MARKER_RE.finditer(full):
            start, end = m.span()
            involved = [i for i, (a, b) in enumerate(bounds) if a < end and b > start]
            if len(involved) < 2:
                continue
            first_i, last_i = involved[0], involved[-1]
            first_t, first_txt = chunks[first_i]
            last_t, last_txt = chunks[last_i]
            prefix = first_txt[: start - bounds[first_i][0]]
            suffix = last_txt[end - bounds[last_i][0] :]
            first_t.text = prefix + m.group(0)
            last_t.text = suffix
            for i in involved[1:-1]:
                chunks[i][0].text = ""


def _marker_runs(doc: DocumentT, token: str) -> list[tuple[BaseOxmlElement, BaseOxmlElement]]:
    """Trouve tous les ``(w:p, w:r)`` dont le texte vaut exactement ``token``.

    Recherche dans tout le corps du document, y compris les formes. Un
    paragraphe-marqueur peut porter plusieurs runs (ex. un run de mise en
    forme vide avant le run de texte) : on retient celui qui porte le texte.
    **Une forme (DrawingML) est souvent dupliquée** (``mc:Choice``/
    ``mc:Fallback``, rendu moderne vs VML) : son marqueur apparaît alors deux
    fois dans le XML, d'où une liste plutôt qu'un seul résultat — les deux
    copies doivent être remplies identiquement, sous peine de laisser le
    marqueur visible selon la version de Word qui ouvre le document.

    Returns:
        list: Les paires ``(w:p, w:r)`` trouvées (vide si le marqueur est absent).
    """
    found = []
    for p in doc.element.body.iter(qn("w:p")):
        for r in p.findall(qn("w:r")):
            t = r.find(qn("w:t"))
            if t is not None and t.text and t.text.strip() == token:
                found.append((p, r))
    return found


def set_inline_marker(doc: DocumentT, token: str, value: object) -> None:
    """Substitution en place d'un marqueur mono-valeur, en forme ou en corps.

    Remplace ``token`` par ``value`` dans le texte de **tous** les ``w:t`` du
    document qui le contiennent (mélangé à du texte statique ou seul) — ne
    fait rien si le marqueur est absent.
    """
    for t in doc.element.body.iter(qn("w:t")):
        if t.text and token in t.text:
            t.text = t.text.replace(token, str(value))


_BOLD_RE = re.compile(r"\*\*(.+?)\*\*")


def _split_bold(text: str) -> list[tuple[str, bool]]:
    """Découpe ``text`` en segments ``(sous-chaîne, gras)`` sur les ``**...**``.

    Returns:
        list[tuple[str, bool]]: Les segments dans l'ordre, jamais vide.
    """
    segments: list[tuple[str, bool]] = []
    pos = 0
    for m in _BOLD_RE.finditer(text):
        if m.start() > pos:
            segments.append((text[pos : m.start()], False))
        segments.append((m.group(1), True))
        pos = m.end()
    if pos < len(text) or not segments:
        segments.append((text[pos:], False))
    return segments


def _set_run_bold(run: BaseOxmlElement, bold: bool) -> None:
    """Active/désactive le gras d'un run, sans toucher au reste de sa mise en forme."""
    rpr = run.find(qn("w:rPr"))
    if rpr is None:
        rpr = OxmlElement("w:rPr")
        run.insert(0, rpr)
    b = rpr.find(qn("w:b"))
    if bold:
        if b is None:
            rpr.append(OxmlElement("w:b"))
    elif b is not None:
        rpr.remove(b)


def _write_line_runs(p_el: BaseOxmlElement, template_run: BaseOxmlElement, text: str) -> None:
    """Remplace tous les ``w:r`` de ``p_el`` par 1 run par segment ``**gras**``/normal.

    ``template_run`` (jamais modifié) fournit la mise en forme de base (police,
    taille…) de chaque nouveau run — seul le gras varie d'un segment à l'autre.
    """
    for old in list(p_el.findall(qn("w:r"))):
        p_el.remove(old)
    for seg_text, bold in _split_bold(text):
        run = copy.deepcopy(template_run)
        for child in list(run):
            if child.tag == qn("w:br"):
                run.remove(child)
        t = run.find(qn("w:t"))
        if t is None:
            t = OxmlElement("w:t")
            run.append(t)
        t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        t.text = seg_text
        _set_run_bold(run, bold)
        p_el.append(run)


def _ensure_para_spacing_after(p_el: BaseOxmlElement, after: int = 160) -> None:
    """Garantit un espacement après le paragraphe (twentièmes de point, 160 = 8pt)."""
    ppr = p_el.find(qn("w:pPr"))
    if ppr is None:
        ppr = OxmlElement("w:pPr")
        p_el.insert(0, ppr)
    spacing = ppr.find(qn("w:spacing"))
    if spacing is None:
        spacing = OxmlElement("w:spacing")
        ppr.append(spacing)
    spacing.set(qn("w:after"), str(after))


def fill_shape_lines(doc: DocumentT, token: str, lines: list[str] | None) -> None:
    """Remplit un marqueur multi-lignes dans une forme, un paragraphe par ligne.

    Chaque paragraphe marqueur trouvé (cf. ``_marker_runs`` — souvent 2,
    ``mc:Choice``/``mc:Fallback``) ne porte que le token : la 1ʳᵉ ligne
    réutilise son paragraphe, les suivantes sont des **clones** de ce même
    paragraphe (même mise en forme), insérés juste après — de vrais
    paragraphes, pas des ``w:br`` dans un seul run, avec un espacement après
    chaque paragraphe (cf. ``_ensure_para_spacing_after``). Une ligne peut
    marquer du texte en gras avec ``**...**`` (converti en runs séparés, cf.
    ``_write_line_runs`` — jamais rendu littéralement). Texte de repli si
    ``lines`` est vide.
    """
    items = [str(line) for line in lines or []] or ["À compléter."]
    for p_el, run in _marker_runs(doc, token):
        template_run = copy.deepcopy(run)
        _ensure_para_spacing_after(p_el)
        _write_line_runs(p_el, template_run, items[0])
        anchor = p_el
        for line in items[1:]:
            clone = copy.deepcopy(p_el)
            _write_line_runs(clone, template_run, line)
            anchor.addnext(clone)
            anchor = clone


def fill_shape_image(
    doc: DocumentT,
    token: str,
    image_path: str | Path | None,
    *,
    width_in: float = 6.0,
) -> None:
    """Insère une image à la place d'un marqueur mono-run dans une forme.

    Vide le run marqueur puis ajoute un **nouveau run** (jamais de paragraphe)
    au paragraphe existant, portant l'image. Écrit « (visuel indisponible) »
    dans le run vidé si ``image_path`` est absent. Traite toutes les
    occurrences trouvées (cf. ``_marker_runs`` — souvent 2, ``mc:Choice``/
    ``mc:Fallback``).
    """
    for p_el, run in _marker_runs(doc, token):
        t = run.find(qn("w:t"))
        if not image_path or not Path(image_path).exists():
            if t is not None:
                t.text = "(visuel indisponible)"
            continue
        if t is not None:
            t.text = ""
        paragraph = Paragraph(p_el, doc)
        paragraph.add_run().add_picture(str(image_path), width=Inches(width_in))
