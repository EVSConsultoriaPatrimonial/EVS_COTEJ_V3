
"""
relatorio_pdf_v2.py (EVS)
---------------------------------
Módulo de geração de PDF executivo do Dashboard.

✅ Compatível com chamadas antigas e novas:
    gerar_relatorio_pdf()
    gerar_relatorio_pdf(db_path)
    gerar_relatorio_pdf(db_path, dados)
    gerar_relatorio_pdf(db_path, dados, output_path="...")

Requisitos:
- reportlab
- matplotlib (opcional, usado para gráficos)

Observação:
Este módulo tenta detectar automaticamente nomes de colunas comuns
nas tabelas BsContabil / BsFisico / BsDePara.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional, Tuple, List
from db_utils_v2 import connect

# PDF
try:
    # PDF (opcional — o sistema deve abrir mesmo sem reportlab)
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.pdfgen import canvas
    from reportlab.lib.utils import ImageReader
    from reportlab.lib.units import mm
except Exception:  # pragma: no cover
    A4 = landscape = canvas = ImageReader = mm = None

def _ensure_reportlab() -> None:
    if A4 is None or landscape is None or canvas is None or ImageReader is None or mm is None:
        raise RuntimeError(
            "Biblioteca 'reportlab' não está instalada.\n\n"
            "Para habilitar o PDF, instale com:\n"
            "  python -m pip install reportlab\n\n"
            "Depois, reabra o sistema."
        )

# Gráficos (opcional, mas normalmente disponível)
try:
    import matplotlib.pyplot as plt
except Exception:  # pragma: no cover
    plt = None


# -----------------------------
# Helpers de formatação (pt-BR)
# -----------------------------
def _br_money(v: float) -> str:
    try:
        s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return f"R$ {v}"

def _br_int(v: int) -> str:
    try:
        return f"{int(v):,}".replace(",", ".")
    except Exception:
        return str(v)

def _br_pct(ratio: float) -> str:
    try:
        return f"{ratio*100:.2f}%".replace(".", ",")
    except Exception:
        return "0,00%"


# -----------------------------
# SQLite utils
# -----------------------------
def _connect(db_path: str) -> sqlite3.Connection:
    return connect(db_path)

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def _columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in cur.fetchall()]

def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    lower = {c.lower(): c for c in cols}
    for cand in candidates:
        key = cand.lower()
        if key in lower:
            return lower[key]
    # tentativa: normalize removendo espaços, pontos e underscores
    def norm(s: str) -> str:
        return "".join(ch for ch in s.lower() if ch.isalnum())
    norm_map = {norm(c): c for c in cols}
    for cand in candidates:
        k = norm(cand)
        if k in norm_map:
            return norm_map[k]
    return None

def _safe_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        return float(x)
    except Exception:
        return 0.0

def _safe_int(x: Any) -> int:
    try:
        if x is None:
            return 0
        return int(float(x))
    except Exception:
        return 0


# -----------------------------
# Coleta de métricas
# -----------------------------
def _fetch_metrics(db_path: str) -> Dict[str, Any]:
    """
    Retorna um dicionário com métricas principais e tabelas agregadas.
    """
    out: Dict[str, Any] = {
        "db_path": db_path,
        "ts": datetime.now(),
        "has_ctb": False,
        "has_fis": False,
        "has_depara": False,
    }

    with _connect(db_path) as conn:
        out["has_ctb"] = _table_exists(conn, "BsContabil")
        out["has_fis"] = _table_exists(conn, "BsFisico")
        out["has_depara"] = _table_exists(conn, "BsDePara") or _table_exists(conn, "BsDepara") or _table_exists(conn, "BsDePARA")
        depara_table = "BsDePara" if _table_exists(conn, "BsDePara") else ("BsDepara" if _table_exists(conn, "BsDepara") else ("BsDePARA" if _table_exists(conn, "BsDePARA") else None))

        # Contábil
        if out["has_ctb"]:
            cols = _columns(conn, "BsContabil")
            col_cod = _pick_col(cols, ["COD_CONTA", "CONTA", "CODCONTA", "COD CONTA"])
            col_res = _pick_col(cols, ["VLR_RESIDUAL", "VLR. RESIDUAL", "VALOR_RESIDUAL", "RESIDUAL", "VLR RESIDUAL"])
            col_aq  = _pick_col(cols, ["VLR_AQUISICAO", "VLR. AQUISICAO", "VLR AQUISICAO", "AQUISICAO", "VALOR_AQUISICAO", "VLR AQUISIÇÃO", "VLR. AQUISIÇÃO"])
            col_dep = _pick_col(cols, ["DEP_ACUMULADA", "DEP. ACUMULADA", "DEPR_ACUMULADA", "DEPRECIACAO_ACUMULADA", "DEPR. ACUMULADA", "DEP ACUMULADA"])

            # totais
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM BsContabil")
            out["ctb_qtd_total"] = _safe_int(cur.fetchone()[0])

            if col_res:
                q = f"SELECT COALESCE(SUM(CAST({col_res} AS REAL)),0) FROM BsContabil"
                cur.execute(q)
                out["ctb_residual_total"] = _safe_float(cur.fetchone()[0])
            else:
                out["ctb_residual_total"] = 0.0

            # agregação por conta (top 10 por residual)
            if col_cod and col_res:
                q = f"""
                    SELECT {col_cod} AS conta,
                           COALESCE(SUM(CAST({col_res} AS REAL)),0) AS residual
                    FROM BsContabil
                    GROUP BY {col_cod}
                    ORDER BY residual DESC
                    LIMIT 12
                """
                cur.execute(q)
                out["ctb_residual_por_conta"] = [(str(r[0]), _safe_float(r[1])) for r in cur.fetchall()]
            else:
                out["ctb_residual_por_conta"] = []

        # Físico
        if out["has_fis"]:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM BsFisico")
            out["fis_qtd_total"] = _safe_int(cur.fetchone()[0])

            cols = _columns(conn, "BsFisico")
            col_qtd = _pick_col(cols, ["QTD", "QTDE", "QUANTIDADE"])
            if col_qtd:
                cur.execute(f"SELECT COALESCE(SUM(CAST({col_qtd} AS REAL)),0) FROM BsFisico")
                out["fis_qtd_soma"] = _safe_float(cur.fetchone()[0])
            else:
                out["fis_qtd_soma"] = float(out.get("fis_qtd_total", 0))

        # De-Para / Conciliados
        if depara_table:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*) FROM {depara_table}")
            out["depara_qtd"] = _safe_int(cur.fetchone()[0])

        # Ratios simples (fallback: baseado em contagens)
        ctb_total = float(out.get("ctb_residual_total", 0.0))
        # Se tiver residual, a lógica de % no PDF usa residual; senão usa contagem
        out["use_residual"] = bool(out.get("ctb_residual_total", 0.0) > 0.0)

    return out


# -----------------------------
# Geração de gráficos (PNG)
# -----------------------------
def _make_charts(metrics: Dict[str, Any], workdir: str) -> Dict[str, str]:
    """
    Cria gráficos simples e retorna paths de imagens.
    """
    imgs: Dict[str, str] = {}
    if plt is None:
        return imgs

    os.makedirs(workdir, exist_ok=True)

    # Donut contábil (residual)
    total_res = float(metrics.get("ctb_residual_total", 0.0))
    # sem acesso ao residual conciliado/sobras com precisão sem coluna FRAG/flag no SQL.
    # aqui, usamos proporção por contagem como aproximação, caso exista.
    depara = int(metrics.get("depara_qtd", 0))
    ctb_qtd = int(metrics.get("ctb_qtd_total", 0))
    ratio = (depara / ctb_qtd) if ctb_qtd else 0.0
    if total_res <= 0:
        # se não tem residual, basear total em contagem mesmo
        pass

    fig = plt.figure(figsize=(5.2, 3.2), dpi=160)
    ax = fig.add_subplot(111)
    conc = ratio
    sob = max(0.0, 1.0 - conc)
    vals = [conc, sob]
    labels = ["Conciliados", "Sobras"]
    colors = ["#2aa198", "#f59e0b"]
    wedges, _ = ax.pie(vals, startangle=90, colors=colors, wedgeprops=dict(width=0.38, edgecolor="#0f3b3b"))
    ax.text(0, 0, f"{(conc*100):.2f}%\nConciliados".replace(".", ","), ha="center", va="center", color="white", fontsize=12, fontweight="bold")
    ax.legend(wedges, labels, loc="lower center", bbox_to_anchor=(0.5, -0.08), ncol=2, frameon=False, fontsize=9)
    ax.set_facecolor("#0f3b3b")
    fig.patch.set_facecolor("#0f3b3b")
    p1 = os.path.join(workdir, "donut_ctb.png")
    fig.savefig(p1, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    imgs["donut_ctb"] = p1

    # Bar por conta (residual)
    data = metrics.get("ctb_residual_por_conta", [])
    if data:
        contas = [c for c, _ in data][::-1]
        vals = [v for _, v in data][::-1]
        fig = plt.figure(figsize=(7.0, 3.6), dpi=160)
        ax = fig.add_subplot(111)
        ax.barh(range(len(contas)), vals, color="#2aa198")
        ax.set_yticks(range(len(contas)))
        ax.set_yticklabels(contas, fontsize=9, color="white")
        ax.tick_params(axis="x", colors="white")
        ax.set_title("Base Contábil — Residual por Conta (Top)", color="white", fontsize=12, pad=10)
        ax.grid(alpha=0.15)
        ax.set_facecolor("#0f3b3b")
        fig.patch.set_facecolor("#0f3b3b")
        p2 = os.path.join(workdir, "bar_ctb_conta.png")
        fig.savefig(p2, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        imgs["bar_ctb_conta"] = p2

    return imgs


# -----------------------------
# PDF layout
# -----------------------------
def _draw_card(c: canvas.Canvas, x: float, y: float, w: float, h: float, title: str,
               rows: List[Tuple[str, str, str]], note: str = ""):
    """
    Desenha um card com 'linhas' (label, valor, %).
    """
    # sombra
    c.setFillColorRGB(0.03, 0.15, 0.15)  # shadow
    c.roundRect(x+2, y-2, w, h, 8, fill=1, stroke=0)

    # card
    c.setFillColorRGB(0.05, 0.21, 0.21)
    c.roundRect(x, y, w, h, 8, fill=1, stroke=0)

    # título
    c.setFillColorRGB(0.85, 0.93, 0.93)
    c.setFont("Helvetica", 10)
    c.drawString(x + 10, y + h - 18, title)

    # corpo
    c.setFont("Helvetica-Bold", 9)
    base_y = y + h - 38
    line_h = 14
    for i, (lab, val, pct) in enumerate(rows):
        yy = base_y - i*line_h
        c.setFillColorRGB(1, 1, 1)
        c.drawString(x + 10, yy, lab)
        c.setFillColorRGB(1, 1, 1)
        c.drawRightString(x + w - 70, yy, val)
        c.setFillColorRGB(0.80, 0.88, 0.88)
        c.drawRightString(x + w - 12, yy, pct)

    if note:
        c.setFont("Helvetica", 7.5)
        c.setFillColorRGB(0.75, 0.85, 0.85)
        c.drawString(x + 10, y + 10, note)


def gerar_relatorio_pdf(db_path: Optional[str] = None,
                       dados: Optional[Dict[str, Any]] = None,
                       output_path: Optional[str] = None,
                       *args, **kwargs) -> str:
    """
    Gera um PDF executivo do dashboard.

    Compatível com versões que chamam gerar_relatorio_pdf() sem argumentos,
    e com versões que chamam gerar_relatorio_pdf(db_path, dados).

    Retorna o caminho do PDF gerado.
    """
    # Compatibilidade com chamadas antigas: se db_path veio em 'dados' por engano
    if db_path is None and isinstance(dados, str):
        db_path, dados = dados, None

    if db_path is None:
        raise ValueError("db_path não informado. Informe o caminho do arquivo .db do SQLite.")

    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Banco não encontrado: {db_path}")

    # Pasta de saída padrão: ao lado do DB
    if output_path is None:
        base = os.path.splitext(os.path.basename(db_path))[0]
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(os.path.dirname(db_path), f"Relatorio_Dashboard_{base}_{ts}.pdf")

    metrics = _fetch_metrics(db_path)

    # Workdir temporário para imagens
    workdir = os.path.join(os.path.dirname(output_path), "_tmp_pdf_imgs")
    imgs = _make_charts(metrics, workdir)

    _ensure_reportlab()

    # --- PDF ---
    page = landscape(A4)
    c = canvas.Canvas(output_path, pagesize=page)
    W, H = page

    # Fundo
    c.setFillColorRGB(0.06, 0.23, 0.23)
    c.rect(0, 0, W, H, fill=1, stroke=0)

    # Header
    c.setFillColorRGB(1, 1, 1)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(18, H - 24, "EVS — Dashboard Executivo")
    c.setFont("Helvetica", 9)
    c.setFillColorRGB(0.85, 0.93, 0.93)
    c.drawString(18, H - 40, f"DB: {os.path.basename(db_path)} | Gerado em: {metrics['ts'].strftime('%d/%m/%Y %H:%M:%S')}")

    # Cards (topo)
    card_w = (W - 18*2 - 12) / 2
    card_h = 62
    y_cards = H - 40 - card_h - 10

    # Contábil
    ctb_total = float(metrics.get("ctb_residual_total", 0.0))
    depara = int(metrics.get("depara_qtd", 0))
    ctb_qtd = int(metrics.get("ctb_qtd_total", 0))
    conc_ratio = (depara / ctb_qtd) if ctb_qtd else 0.0
    sob_ratio = max(0.0, 1.0 - conc_ratio)

    rows_ctb = [
        ("TOTAL", _br_money(ctb_total) if ctb_total > 0 else _br_int(ctb_qtd), "100,00%"),
        ("Conciliados", _br_money(ctb_total * conc_ratio) if ctb_total > 0 else _br_int(depara), _br_pct(conc_ratio)),
        ("Sobras", _br_money(ctb_total * sob_ratio) if ctb_total > 0 else _br_int(max(0, ctb_qtd - depara)), _br_pct(sob_ratio)),
    ]
    _draw_card(c, 18, y_cards, card_w, card_h, "Base Contábil — Residual", rows_ctb,
               note="(Conta: Ativo Imobilizado — usa COD_CONTA quando disponível)")

    # Físico
    fis_total = float(metrics.get("fis_qtd_soma", metrics.get("fis_qtd_total", 0)))
    fis_qtd_total = int(metrics.get("fis_qtd_total", 0))
    rows_fis = [
        ("TOTAL", _br_int(int(round(fis_total))) if fis_total else _br_int(fis_qtd_total), "100,00%"),
        ("Conciliados", _br_int(depara), _br_pct(conc_ratio)),
        ("Sobras", _br_int(max(0, fis_qtd_total - depara)), _br_pct(max(0.0, 1.0 - conc_ratio))),
    ]
    _draw_card(c, 18 + card_w + 12, y_cards, card_w, card_h, "Base Física — Quantidade", rows_fis,
               note="(Sem contas e sem valores — usa QTD quando disponível)")

    # Área de gráficos
    y_graph = y_cards - 10 - 180
    x_left = 18
    x_right = 18 + card_w + 12

    def _draw_img(key: str, x: float, y: float, w: float, h: float):
        p = imgs.get(key)
        if not p or not os.path.exists(p):
            # placeholder
            c.setFillColorRGB(0.05, 0.21, 0.21)
            c.roundRect(x, y, w, h, 8, fill=1, stroke=0)
            c.setFillColorRGB(0.85, 0.93, 0.93)
            c.setFont("Helvetica", 9)
            c.drawString(x + 10, y + h - 18, "Gráfico indisponível")
            return
        c.drawImage(ImageReader(p), x, y, width=w, height=h, preserveAspectRatio=True, mask='auto')

    # Donut contábil (esquerda) + barras por conta (direita)
    _draw_img("donut_ctb", x_left, y_graph, card_w*0.48, 170)
    _draw_img("bar_ctb_conta", x_left + card_w*0.52, y_graph, card_w*0.48, 170)

    # Rodapé
    c.setFillColorRGB(0.85, 0.93, 0.93)
    c.setFont("Helvetica", 8)
    c.drawRightString(W - 18, 14, "EVS Consultoria Patrimonial e Avaliações Ltda. — Relatório Executivo")

    c.showPage()
    c.save()

    # Limpa imagens temporárias (best-effort)
    try:
        for p in imgs.values():
            if os.path.exists(p):
                os.remove(p)
        if os.path.isdir(workdir) and not os.listdir(workdir):
            os.rmdir(workdir)
    except Exception:
        pass

    return output_path
