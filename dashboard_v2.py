# dashboard_v2.py
# Dashboard Executivo — Layout em grade (2 colunas) + Cards
# - Base Contábil: usa COD_CONTA (código da conta do ativo imobilizado)
# - Base Física: sem contas / sem valores -> apenas QTD
# - Mostra séries completas: TOTAL, Conciliados, Sobras (contábil) e TOTAL/Conciliados/Sobras (físico)
# - Percentuais formatados como 0,00%
# - Sem lista de itens

from __future__ import annotations

import os
import sqlite3
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox
from tkinter import ttk
from db_utils_v2 import connect

# --- Matplotlib (necessário para dashboards) ---
try:
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    import matplotlib.pyplot as plt
    from matplotlib.figure import Figure
    _MATPLOTLIB_ERR = None
except Exception as e:
    FigureCanvasTkAgg = None  # type: ignore
    plt = None  # type: ignore
    Figure = None  # type: ignore
    _MATPLOTLIB_ERR = e

BG = "#225781"
CARD_BG = "#0d3535"
CARD_FG = "white"
MUTED = "#cfe8e8"

# ----------------- helpers -----------------

def _maximize_window(win: tk.Misc) -> None:
    """Abre a janela maximizada (Windows/macOS/Linux)."""
    try:
        win.state("zoomed")
        return
    except Exception:
        pass
    try:
        win.attributes("-zoomed", True)
        return
    except Exception:
        pass
    try:
        w, h = win.winfo_screenwidth(), win.winfo_screenheight()
        win.geometry(f"{w}x{h}+0+0")
    except Exception:
        pass


def _connect(db_path: str) -> sqlite3.Connection:
    return connect(db_path)

def _safe_float(x) -> float:
    try:
        return float(x) if x is not None else 0.0
    except Exception:
        return 0.0

def _safe_int(x) -> int:
    try:
        return int(float(x)) if x is not None else 0
    except Exception:
        return 0

def _br_money(v: float) -> str:
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

def _br_pct(v: float) -> str:
    # v em [0..1]
    return (f"{v*100:.2f}%").replace(".", ",")

def _abbr(s: str, limit: int = 28) -> str:
    s = (s or "").strip()
    if len(s) <= limit:
        return s
    return s[:limit - 3].rstrip() + "..."

def _q1(cur: sqlite3.Cursor, sql: str, params=()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return row[0] if row else None

def _qall(cur: sqlite3.Cursor, sql: str, params=()):
    cur.execute(sql, params)
    return cur.fetchall()

# ----------------- UI helpers (rounded cards) -----------------

def _round_rect(canvas: tk.Canvas, x1, y1, x2, y2, r=16, **kwargs):
    """Draw a rounded rectangle on a Canvas."""
    points = [
        x1 + r, y1,
        x2 - r, y1,
        x2, y1,
        x2, y1 + r,
        x2, y2 - r,
        x2, y2,
        x2 - r, y2,
        x1 + r, y2,
        x1, y2,
        x1, y2 - r,
        x1, y1 + r,
        x1, y1
    ]
    return canvas.create_polygon(points, smooth=True, **kwargs)

class RoundedCard(tk.Frame):
    def __init__(self, master, *, bg=BG, card_bg=CARD_BG, outline=None, radius=22, padx=12, pady=10):
        super().__init__(master, bg=bg)
        self._radius = radius
        self._card_bg = card_bg
        self._outline = outline
        self._padx = padx
        self._pady = pady

        self.canvas = tk.Canvas(self, bg=bg, highlightthickness=0, bd=0)
        self.canvas.pack(fill="both", expand=True)

        self.inner = tk.Frame(self.canvas, bg=card_bg, bd=0, highlightthickness=0)
        self._win = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

        self.canvas.bind("<Configure>", self._on_resize)

    def _on_resize(self, event):
        w, h = event.width, event.height
        self.canvas.delete("card")
        _round_rect(
            self.canvas, 2, 2, w - 2, h - 2, r=self._radius,
            fill=self._card_bg,
            outline=(self._outline if self._outline else self._card_bg),
            width=(1 if self._outline else 0),
            tags=("card",)
        )
        self.canvas.coords(self._win, self._padx, self._pady)
        self.canvas.itemconfigure(self._win, width=max(0, w - 2 * self._padx), height=max(0, h - 2 * self._pady))

# ----------------- consultas -----------------

def _where_filters(alias: str, filial: str | None = None, ccusto: str | None = None) -> tuple[str, list]:
    """Monta filtro SQL por Filial/CCusto. Use '(TODAS)' para não filtrar."""
    conds = []
    params: list = []
    if filial and filial != '(TODAS)':
        conds.append(f"TRIM(CAST({alias}.FILIAL AS TEXT)) = TRIM(?)")
        params.append(filial)
    if ccusto and ccusto != '(TODAS)':
        conds.append(f"TRIM(CAST({alias}.CCUSTO AS TEXT)) = TRIM(?)")
        params.append(ccusto)
    if not conds:
        return "", []
    return " AND " + " AND ".join(conds), params

def _distinct_values(con: sqlite3.Connection, table: str, col: str) -> list[str]:
    cur = con.cursor()
    try:
        rows = cur.execute(f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS TEXT)) <> '' ORDER BY {col}").fetchall()
        return [str(r[0]) for r in rows if r and r[0] is not None]
    except Exception:
        return []

def _conta_expr() -> str:
    return "COALESCE(NULLIF(TRIM(COD_CONTA), ''), '(SEM CONTA)')"

def contabil_agregado(con: sqlite3.Connection, scope: str, top_n: int | None = None, *, filial: str | None = None, ccusto: str | None = None):
    cur = con.cursor()

    filt_sql, params = _where_filters("t", filial=filial, ccusto=ccusto)

    if scope == "CONC":
        where = "WHERE EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='CTB' AND c.ID=t.ID)" + filt_sql
    elif scope == "Sobras":
        where = "WHERE NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='CTB' AND c.ID=t.ID)" + filt_sql
    else:
        where = ("WHERE 1=1" + filt_sql) if filt_sql else ""

    sql = f"""
        SELECT {_conta_expr()} AS conta,
               SUM(COALESCE(t.VLR_AQUISICAO,0)) AS aquis,
               SUM(COALESCE(t.DEP_ACUMULADA,0)) AS dep,
               SUM(COALESCE(t.VLR_RESIDUAL,0)) AS resid,
               SUM(COALESCE(t.QTD,1)) AS qtd
        FROM contabil t
        {where}
        GROUP BY conta
        ORDER BY resid DESC
    """
    rows = _qall(cur, sql, params)
    out = []
    use = rows if top_n is None else rows[:top_n]
    for r in use:
        out.append({
            "conta": r[0],
            "aquis": _safe_float(r[1]),
            "dep": _safe_float(r[2]),
            "resid": _safe_float(r[3]),
            "qtd": _safe_int(r[4]),
        })
    return out

def contabil_totais_residual(con: sqlite3.Connection, *, filial: str | None = None, ccusto: str | None = None):
    cur = con.cursor()
    filt_sql, params = _where_filters("", filial=filial, ccusto=ccusto)
    # _where_filters com alias vazio retorna condições como '.FILIAL', então aqui usamos alias 't'
    filt_sql_t, params_t = _where_filters("t", filial=filial, ccusto=ccusto)

    total = _safe_float(_q1(cur, f"SELECT SUM(COALESCE(t.VLR_RESIDUAL,0)) FROM contabil t WHERE 1=1{filt_sql_t}", params_t) or 0.0)
    conc = _safe_float(_q1(cur, f"""
        SELECT SUM(COALESCE(t.VLR_RESIDUAL,0))
        FROM contabil t
        WHERE EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='CTB' AND c.ID=t.ID){filt_sql_t}
    """, params_t) or 0.0)
    sobra = max(0.0, total - conc)
    return total, conc, sobra

def contabil_totais_qtd(con: sqlite3.Connection, *, filial: str | None = None, ccusto: str | None = None):
    cur = con.cursor()
    filt_sql_t, params_t = _where_filters("t", filial=filial, ccusto=ccusto)

    total = _safe_int(_q1(cur, f"SELECT SUM(COALESCE(QTD,1)) FROM contabil t WHERE 1=1{filt_sql_t}", params_t) or 0)
    conc = _safe_int(_q1(cur, f"""
        SELECT SUM(COALESCE(t.QTD,1))
        FROM contabil t
        WHERE EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='CTB' AND c.ID=t.ID){filt_sql_t}
    """, params_t) or 0)
    sobra = max(0, total - conc)
    return total, conc, sobra

def fisico_totais_qtd(con: sqlite3.Connection, *, filial: str | None = None, ccusto: str | None = None):
    cur = con.cursor()
    filt_sql_f, params_f = _where_filters("f", filial=filial, ccusto=ccusto)

    total = _safe_int(_q1(cur, f"SELECT SUM(COALESCE(QTD,1)) FROM fisico f WHERE 1=1{filt_sql_f}", params_f) or 0)
    conc = _safe_int(_q1(cur, f"""
        SELECT SUM(COALESCE(f.QTD,1))
        FROM fisico f
        WHERE EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='FIS' AND c.ID=f.ID){filt_sql_f}
    """, params_f) or 0)
    sobra = max(0, total - conc)
    return total, conc, sobra

# ----------------- UI Scroll + Grid -----------------

class _ScrollFrame(tk.Frame):
    def __init__(self, master: tk.Misc, **kwargs):
        super().__init__(master, **kwargs)
        self.canvas = tk.Canvas(self, highlightthickness=0, bd=0, bg=self["bg"])
        self.vbar = tk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.vbar.set)
        self.inner = tk.Frame(self.canvas, bg=self["bg"])
        self.window_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

        self.canvas.pack(side="left", fill="both", expand=True)
        self.vbar.pack(side="right", fill="y")

        self.inner.bind("<Configure>", self._on_inner_configure)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        self.canvas.bind("<Enter>", lambda _e: self.canvas.focus_set())
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Shift-MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Button-4>", lambda e: self.canvas.yview_scroll(-1, "units"))
        self.canvas.bind_all("<Button-5>", lambda e: self.canvas.yview_scroll(1, "units"))

    def _on_inner_configure(self, _evt):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, evt):
        self.canvas.itemconfigure(self.window_id, width=evt.width)

    def _on_mousewheel(self, evt):
        d = getattr(evt, "delta", 0) or 0
        if d == 0:
            return
        if abs(d) < 120:
            step = -1 if d > 0 else 1
        else:
            step = int(-d / 120) or (-1 if d > 0 else 1)
        self.canvas.yview_scroll(step, "units")

# ----------------- Dashboard Analítico -----------------

class DashboardWindow(tk.Toplevel):
    def __init__(self, master: tk.Misc, *, db_path: str, excel_hint: str | None = None):
        super().__init__(master)
        _maximize_window(self)

        if FigureCanvasTkAgg is None or plt is None:
            messagebox.showerror(
                "Dashboard",
                "O módulo 'matplotlib' não está instalado neste Python.\n\n"
                "Instale com:\n"
                "/usr/local/bin/python3 -m pip install matplotlib\n\n"
                f"Erro: {_MATPLOTLIB_ERR}"
            )
            self.destroy()
            return

        self.db_path = db_path
        self.excel_hint = excel_hint or ""

        # Filtros (opcional): Filial / Centro de Custo
        self.filial_var = tk.StringVar(value='(TODAS)')
        self.ccusto_var = tk.StringVar(value='(TODAS)')

        self.title("Dashboard Executivo — EVS")
        self.configure(bg=BG)

        # Filtros (opcional): Filial / Centro de Custo
        self.filial_var = tk.StringVar(value='(TODAS)')
        self.ccusto_var = tk.StringVar(value='(TODAS)')
        self.geometry("1400x820")

        self._figs = []
        self._charts = []  # (subtitle, fig)
        # (cont_total, cont_conc, cont_sobra, cont_q_total, cont_q_conc, cont_q_sobra, fis_total, fis_conc, fis_sobra)
        self._last_cards = None
        self._grid_row = 0
        self._grid_col = 0

        self._build()
        self._render_all()

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build(self):
        top = tk.Frame(self, bg=BG)
        top.pack(fill="x", padx=14, pady=(12, 8))

        tk.Label(top, text="DASHBOARD EXECUTIVO", bg=BG, fg="white",
                 font=("Helvetica", 18, "bold")).pack(side="left")

        tk.Label(top, text=f"DB: {os.path.basename(self.db_path)}", bg=BG, fg=MUTED,
                 font=("Helvetica", 9)).pack(side="left", padx=14)

        btns = tk.Frame(top, bg=BG)
        btns.pack(side="right")
        tk.Button(btns, text="Gerar PDF", width=16, command=self._gerar_pdf).pack(side="left", padx=6)
        tk.Button(btns, text="Fechar", width=12, command=self._on_close).pack(side="left", padx=6)

        # Barra de filtros (Filial / CCusto)
        filt = tk.Frame(self, bg=BG)
        filt.pack(fill="x", padx=14, pady=(0, 8))

        tk.Label(filt, text="Filial:", bg=BG, fg="white", font=("Helvetica", 10, "bold")).pack(side="left")
        self.cbo_filial = ttk.Combobox(filt, textvariable=self.filial_var, state="readonly", width=18)
        self.cbo_filial.pack(side="left", padx=(6, 14))

        tk.Label(filt, text="Centro de Custo:", bg=BG, fg="white", font=("Helvetica", 10, "bold")).pack(side="left")
        self.cbo_ccusto = ttk.Combobox(filt, textvariable=self.ccusto_var, state="readonly", width=22)
        self.cbo_ccusto.pack(side="left", padx=(6, 14))

        tk.Button(filt, text="Aplicar Filtro", width=14, command=self._apply_filters).pack(side="left", padx=6)
        tk.Button(filt, text="Limpar", width=10, command=lambda: (self.filial_var.set('(TODAS)'), self.ccusto_var.set('(TODAS)'), self._apply_filters())).pack(side="left", padx=6)

        # Carrega valores de filtro do DB
        try:
            con0 = _connect(self.db_path)
            filiais = ['(TODAS)'] + _distinct_values(con0, 'contabil', 'FILIAL')
            ccustos = ['(TODAS)'] + _distinct_values(con0, 'contabil', 'CCUSTO')
            con0.close()
            self.cbo_filial['values'] = filiais
            self.cbo_ccusto['values'] = ccustos
        except Exception:
            self.cbo_filial['values'] = ['(TODAS)']
            self.cbo_ccusto['values'] = ['(TODAS)']

        body = tk.Frame(self, bg=BG)
        body.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        self.scroll = _ScrollFrame(body, bg=BG)
        self.scroll.pack(fill="both", expand=True)

        self.gridwrap = tk.Frame(self.scroll.inner, bg=BG)
        self.gridwrap.pack(fill="both", expand=True, padx=10, pady=10)
        self.gridwrap.grid_columnconfigure(0, weight=1)
        self.gridwrap.grid_columnconfigure(1, weight=1)

    # ---------- Cards ----------
    def _fix_card_heights(self, *cards: tk.Widget) -> None:
        """Força altura mínima dos cards para não cortar o conteúdo (Tkinter + Canvas)."""
        try:
            self.update_idletasks()
            reqs = []
            for c in cards:
                try:
                    c.update_idletasks()
                    inner = getattr(c, "inner", None)
                    if inner is not None:
                        reqs.append(int(inner.winfo_reqheight()))
                    else:
                        reqs.append(int(c.winfo_reqheight()))
                except Exception:
                    pass
            if not reqs:
                return
            min_h = max(reqs) + 26  # margem extra para o Canvas do RoundedCard
            for c in cards:
                try:
                    c.configure(height=min_h)
                    c.grid_propagate(False)
                except Exception:
                    pass
        except Exception:
            pass


    def _add_cards_header(self, cont_total, cont_conc, cont_sobra, cont_q_total, cont_q_conc, cont_q_sobra, fis_total, fis_conc, fis_sobra):
        card_row = tk.Frame(self.gridwrap, bg=BG)
        card_row.grid(row=self._grid_row, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        card_row.grid_columnconfigure(0, weight=1)
        card_row.grid_columnconfigure(1, weight=1)

        left = self._card_contabil(card_row, cont_total, cont_conc, cont_sobra, cont_q_total, cont_q_conc, cont_q_sobra)
        left.grid(row=0, column=0, sticky="ew", padx=(0, 10))
        right = self._card_fisico(card_row, fis_total, fis_conc, fis_sobra)
        right.grid(row=0, column=1, sticky="ew", padx=(10, 0))

        # Ajusta a altura mínima dos cards (evita cortar 'Sobras' em algumas resoluções)
        self._fix_card_heights(left, right)

        self._grid_row += 1

    def _card_contabil(self, master, total, conc, sobra, q_total, q_conc, q_sobra):
        pct_conc = (conc / total) if total else 0.0
        pct_sobra = (sobra / total) if total else 0.0

        f = RoundedCard(master, bg=BG, card_bg=CARD_BG, outline="#1f5f5f", radius=16)
        tk.Label(f.inner, text="Base Contábil — Residual", bg=CARD_BG, fg=CARD_FG,
                 font=("Helvetica", 12, "bold")).grid(row=0, column=0, sticky="w", padx=10, pady=(6, 2), columnspan=2)

        rows = [
            ("TOTAL", _br_money(total), "100,00%"),
            ("Conciliados", _br_money(conc), _br_pct(pct_conc)),
            ("Sobras", _br_money(sobra), _br_pct(pct_sobra)),
        ]
        for i, (lab, val, pct) in enumerate(rows, start=1):
            tk.Label(f.inner, text=lab, bg=CARD_BG, fg=CARD_FG, font=("Helvetica", 11, "bold")).grid(row=i, column=0, sticky="w", padx=10, pady=1)
            tk.Label(f.inner, text=f"{val}   ({pct})", bg=CARD_BG, fg=CARD_FG, font=("Helvetica", 11)).grid(row=i, column=1, sticky="e", padx=10, pady=1)

        tk.Label(f.inner, text="", bg=CARD_BG).grid(row=4, column=0, columnspan=2, pady=(2, 1))

        pct_q_conc = (q_conc / q_total) if q_total else 0.0
        pct_q_sobra = (q_sobra / q_total) if q_total else 0.0
        rows_q = [
            ("ITENS", f"{q_total}", "100,00%"),
            ("Conciliados", f"{q_conc}", _br_pct(pct_q_conc)),
            ("Sobras", f"{q_sobra}", _br_pct(pct_q_sobra)),
        ]
        for j, (lab, val, pct) in enumerate(rows_q, start=5):
            tk.Label(f.inner, text=lab, bg=CARD_BG, fg=MUTED, font=("Helvetica", 10, "bold")).grid(row=j, column=0, sticky="w", padx=10, pady=1)
            tk.Label(f.inner, text=f"{val}   ({pct})", bg=CARD_BG, fg=MUTED, font=("Helvetica", 10)).grid(row=j, column=1, sticky="e", padx=10, pady=1)

        tk.Label(f.inner, text="(Conta: Ativo Imobilizado — usa COD_CONTA)", bg=CARD_BG, fg=MUTED,
                 font=("Helvetica", 10)).grid(row=8, column=0, sticky="w", padx=10, pady=(4, 6), columnspan=2)

        f.inner.grid_columnconfigure(0, weight=1)
        f.inner.grid_columnconfigure(1, weight=1)
        return f

    def _card_fisico(self, master, total, conc, sobra):
        pct_conc = (conc / total) if total else 0.0
        pct_sobra = (sobra / total) if total else 0.0

        f = RoundedCard(master, bg=BG, card_bg=CARD_BG, outline="#1f5f5f", radius=16)
        tk.Label(f.inner, text="Base Física — Quantidade", bg=CARD_BG, fg=CARD_FG,
                 font=("Helvetica", 12, "bold")).grid(row=0, column=0, sticky="w", padx=10, pady=(6, 2), columnspan=2)

        rows = [
            ("TOTAL", f"{total}", "100,00%"),
            ("Conciliados", f"{conc}", _br_pct(pct_conc)),
            ("Sobras", f"{sobra}", _br_pct(pct_sobra)),
        ]
        for i, (lab, val, pct) in enumerate(rows, start=1):
            tk.Label(f.inner, text=lab, bg=CARD_BG, fg=CARD_FG, font=("Helvetica", 11, "bold")).grid(row=i, column=0, sticky="w", padx=10, pady=1)
            tk.Label(f.inner, text=f"{val}   ({pct})", bg=CARD_BG, fg=CARD_FG, font=("Helvetica", 11)).grid(row=i, column=1, sticky="e", padx=10, pady=1)

        tk.Label(f.inner, text="(Sem contas e sem valores — usa QTD)", bg=CARD_BG, fg=MUTED,
                 font=("Helvetica", 10)).grid(row=4, column=0, sticky="w", padx=10, pady=(3, 6), columnspan=2)
        f.inner.grid_columnconfigure(0, weight=1)
        f.inner.grid_columnconfigure(1, weight=1)
        return f

    # ---------- Grid placement ----------
    def _add_chart(self, subtitle: str, fig):
        wrap = tk.Frame(self.gridwrap, bg=BG)
        wrap.grid(row=self._grid_row, column=self._grid_col, sticky="nsew", padx=8, pady=10)
        tk.Label(wrap, text=subtitle, bg=BG, fg="white", font=("Helvetica", 10, "bold")).pack(anchor="w", pady=(0, 6))

        canvas = FigureCanvasTkAgg(fig, master=wrap)
        canvas.draw()
        w = canvas.get_tk_widget()
        w.configure(bg=BG, highlightthickness=0, bd=0)
        w.pack(fill="both", expand=True)

        self._figs.append(fig)
        self._charts.append((subtitle, fig))

        self._grid_col += 1
        if self._grid_col >= 2:
            self._grid_col = 0
            self._grid_row += 1

    # ---------- Plot helpers ----------
    def _base_fig(self, title: str, *, w=5.8, h=3.0):
        fig = plt.Figure(figsize=(w, h), dpi=110)
        ax = fig.add_subplot(111)
        fig.patch.set_facecolor("#4a4a4a")
        ax.set_facecolor("#4a4a4a")
        ax.set_title(title, fontsize=11, color="white", fontweight="bold", pad=10)
        ax.tick_params(colors="white", labelsize=8)
        for spine in ax.spines.values():
            spine.set_visible(False)
        return fig, ax

    def _fig_valores_por_conta(self, rows, title: str):
        n = len(rows) if rows else 0
        fig_h = max(5.2, 0.26 * n + 2.0)
        fig, ax = self._base_fig(title, w=6.8, h=fig_h)
        if not rows:
            ax.text(0.5, 0.5, "Sem dados", transform=ax.transAxes, ha="center", va="center", color="white")
            return fig

        labels = [_abbr(r["conta"], 22) for r in rows][::-1]
        resid = [r["resid"] for r in rows][::-1]

        import numpy as np
        y = np.arange(len(labels)) * 1.18
        hbar = 0.44
        ax.barh(y, resid, height=hbar, label="Residual")

        ax.set_yticks(y)
        ax.set_yticklabels(labels, color="white")
        ax.set_xticks([])
        ax.tick_params(axis="x", bottom=False, labelbottom=False)
        ax.grid(False)

        ax.legend(loc="upper right", frameon=False, labelcolor="white", fontsize=9)
        fig.subplots_adjust(right=0.96)

        xmax = max(resid) if resid else 1.0
        try:
            ax.set_xlim(0, xmax * 1.35)
        except Exception:
            pass
        dx = xmax * 0.012
        for i in range(len(labels)):
            # Exibe apenas os valores do residual para manter o gráfico legível.
            if resid[i]:
                ax.text(resid[i] + dx, y[i], f"{_br_money(resid[i])}", va="center", color="white", fontsize=7)
        return fig

    def _fig_participacao_percent_por_conta(self, rows, metric: str, title: str):
        n = len(rows) if rows else 0
        fig_h = max(3.0, 0.28 * n + 1.6)
        fig, ax = self._base_fig(title, w=5.8, h=fig_h)
        if not rows:
            ax.text(0.5, 0.5, "Sem dados", transform=ax.transAxes, ha="center", va="center", color="white")
            return fig

        vals = [float(r[metric]) for r in rows]
        total = sum(vals) or 1.0
        pct = [v / total for v in vals]

        labels = [_abbr(r["conta"], 22) for r in rows][::-1]
        pct_rev = pct[::-1]

        import numpy as np
        y = np.arange(len(labels))
        ax.barh(y, pct_rev)

        ax.set_yticks(y)
        ax.set_yticklabels(labels, color="white")
        ax.set_xticks([])
        ax.tick_params(axis="x", bottom=False, labelbottom=False)
        ax.set_xlim(0, max(pct_rev) * 1.20)
        ax.grid(False)
        for i, v in enumerate(pct_rev):
            ax.text(v, y[i], f" {_br_pct(v)}", va="center", color="white", fontsize=8)
        return fig

    def _fig_qtd_por_conta(self, rows, title: str):
        n = len(rows) if rows else 0
        fig_h = max(3.0, 0.26 * n + 1.6)
        fig, ax = self._base_fig(title, w=5.8, h=fig_h)
        if not rows:
            ax.text(0.5, 0.5, "Sem dados", transform=ax.transAxes, ha="center", va="center", color="white")
            return fig

        labels = [_abbr(r["conta"], 22) for r in rows][::-1]
        qtd = [r["qtd"] for r in rows][::-1]

        import numpy as np
        y = np.arange(len(labels))
        ax.barh(y, qtd)

        ax.set_yticks(y)
        ax.set_yticklabels(labels, color="white", fontsize=8)
        ax.set_xticks([])
        ax.tick_params(axis="x", bottom=False, labelbottom=False)
        ax.grid(False)

        xmax = max(qtd) if qtd else 1
        try:
            ax.set_xlim(0, xmax * 1.18)
        except Exception:
            pass
        dx = (xmax * 0.012) if xmax else 0.5

        for i, v in enumerate(qtd):
            ax.text(v + dx, y[i], f"{v}", va="center", color="white", fontsize=8)
        return fig

    def _fig_qtd_percent_por_conta(self, rows, title: str):
        n = len(rows) if rows else 0
        fig_h = max(3.0, 0.26 * n + 1.6)
        fig, ax = self._base_fig(title, w=5.8, h=fig_h)
        if not rows:
            ax.text(0.5, 0.5, "Sem dados", transform=ax.transAxes, ha="center", va="center", color="white")
            return fig

        qtd = [r["qtd"] for r in rows]
        total = sum(qtd) or 1
        pct = [q / total for q in qtd]

        labels = [_abbr(r["conta"], 22) for r in rows][::-1]
        pct_rev = pct[::-1]

        import numpy as np
        y = np.arange(len(labels))
        ax.barh(y, pct_rev)

        ax.set_yticks(y)
        ax.set_yticklabels(labels, color="white", fontsize=8)
        ax.set_xticks([])
        ax.tick_params(axis="x", bottom=False, labelbottom=False)
        ax.grid(False)

        xmax = max(pct_rev) if pct_rev else 1.0
        try:
            ax.set_xlim(0, xmax * 1.22)
        except Exception:
            pass
        dx = (xmax * 0.012) if xmax else 0.01
        for i, v in enumerate(pct_rev):
            ax.text(v + dx, y[i], _br_pct(v), va="center", color="white", fontsize=8)
        return fig


    def _reset_view(self):
        """Limpa cards/gráficos para re-renderizar com filtros."""
        try:
            for w in list(self.gridwrap.winfo_children()):
                w.destroy()
        except Exception:
            pass
        self._figs = []
        self._charts = []
        self._last_cards = None
        self._grid_row = 0
        self._grid_col = 0

    def _apply_filters(self):
        # Recalcula com os filtros atuais
        self._reset_view()
        self._render_all()

    def _render_all(self):
        if not self.db_path or not os.path.exists(self.db_path):
            messagebox.showwarning("Dashboard", "Banco de dados não encontrado.")
            return

        con = _connect(self.db_path)
        try:
            cont_total, cont_conc, cont_sobra = contabil_totais_residual(con, filial=self.filial_var.get(), ccusto=self.ccusto_var.get())
            cont_q_total, cont_q_conc, cont_q_sobra = contabil_totais_qtd(con, filial=self.filial_var.get(), ccusto=self.ccusto_var.get())
            fis_total, fis_conc, fis_sobra = fisico_totais_qtd(con, filial=self.filial_var.get(), ccusto=self.ccusto_var.get())

            self._last_cards = (
                cont_total, cont_conc, cont_sobra,
                cont_q_total, cont_q_conc, cont_q_sobra,
                fis_total, fis_conc, fis_sobra
            )

            self._add_cards_header(cont_total, cont_conc, cont_sobra, cont_q_total, cont_q_conc, cont_q_sobra, fis_total, fis_conc, fis_sobra)

            rows_all = contabil_agregado(con, "ALL", top_n=None, filial=self.filial_var.get(), ccusto=self.ccusto_var.get())
            self._add_chart("TOTAL CTB — Valores (Aquisição / Residual) por Conta", self._fig_valores_por_conta(rows_all, "Total da Base Contábil em R$ (por Conta)"))
            self._add_chart("TOTAL CTB — Participação (%) por Conta (Residual)", self._fig_participacao_percent_por_conta(rows_all, "resid", "Total da Base Contábil em % (por Conta)"))
            self._add_chart("TOTAL CTB — Quantidade (QTD) por Conta", self._fig_qtd_por_conta(rows_all, "Total da Base Contábil Quantidade. (por Conta)"))
            self._add_chart("TOTAL CTB — Quantidade (%) por Conta", self._fig_qtd_percent_por_conta(rows_all, "Total da Base Contábil Quantidade. % (por Conta)"))

            rows_conc = contabil_agregado(con, "CONC", top_n=None, filial=self.filial_var.get(), ccusto=self.ccusto_var.get())
            self._add_chart("Conciliados CTB — Valores por Conta (Aquisição / Residual)", self._fig_valores_por_conta(rows_conc, "Base Contábil Conciliada em R$ (por Conta)"))
            self._add_chart("Conciliados CTB — Participação (%) por Conta (Residual)", self._fig_participacao_percent_por_conta(rows_conc, "resid", "Base Contábil Conciliada em % (por Conta)"))
            self._add_chart("Conciliados CTB — Quantidade (QTD) por Conta", self._fig_qtd_por_conta(rows_conc, "Base Contábil Conciliada — Quantidade (por Conta)"))
            self._add_chart("Conciliados CTB — Quantidade (%) por Conta", self._fig_qtd_percent_por_conta(rows_conc, "Base Contábil Conciliada — Quantidade % (por Conta)"))

            rows_sobra = contabil_agregado(con, "Sobras", top_n=None, filial=self.filial_var.get(), ccusto=self.ccusto_var.get())
            self._add_chart("SOBRAS CTB — Valores por Conta (Aquisição / Residual)", self._fig_valores_por_conta(rows_sobra, "Sobras Contábeis em R$ (por Conta)"))
            self._add_chart("SOBRAS CTB — Participação (%) por Conta (Residual)", self._fig_participacao_percent_por_conta(rows_sobra, "resid", "Sobras Contábeis em % (por Conta)"))
            self._add_chart("SOBRAS CTB — Quantidade (QTD) por Conta", self._fig_qtd_por_conta(rows_sobra, "Sobras Contábeis — Quantidade (por Conta)"))
            self._add_chart("SOBRAS CTB — Quantidade (%) por Conta", self._fig_qtd_percent_por_conta(rows_sobra, "Sobras Contábeis — Quantidade % (por Conta)"))
        except Exception as e:
            try:
                messagebox.showerror('Dashboard', f'Falha ao aplicar filtro/gerar gráficos:\n{e}')
            except Exception:
                pass
            return
        finally:
            con.close()

    def _export_pdf_print(self, output_pdf: str):
        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.pdfgen import canvas as pdf_canvas
            from reportlab.lib.utils import ImageReader
        except Exception as e:
            raise RuntimeError(f"reportlab não disponível: {e}")

        if not self._last_cards:
            self._render_all()
        if not self._last_cards:
            raise RuntimeError("Não foi possível calcular os totais para o cabeçalho do PDF.")

        (cont_total, cont_conc, cont_sobra,
         cont_q_total, cont_q_conc, cont_q_sobra,
         fis_total, fis_conc, fis_sobra) = self._last_cards

        tmp_dir = os.path.join(os.path.dirname(output_pdf) or ".", "_tmp_dashboard_pdf")
        os.makedirs(tmp_dir, exist_ok=True)

        chart_imgs = []
        for idx, (subtitle, fig) in enumerate(self._charts, start=1):
            p = os.path.join(tmp_dir, f"chart_{idx:02d}.png")
            fig.savefig(p, dpi=160, bbox_inches="tight")
            chart_imgs.append((subtitle, p))

        W, H = landscape(A4)
        c = pdf_canvas.Canvas(output_pdf, pagesize=landscape(A4))

        def _rgb(hex_):
            hex_ = hex_.lstrip("#")
            return (int(hex_[0:2], 16) / 255.0, int(hex_[2:4], 16) / 255.0, int(hex_[4:6], 16) / 255.0)

        bg_rgb = _rgb(BG)
        card_rgb = _rgb(CARD_BG)
        muted_rgb = _rgb(MUTED)

        margin = 18
        gap = 12
        col_w = (W - 2 * margin - gap) / 2

        def _draw_header():
            c.setFillColorRGB(*bg_rgb)
            c.rect(0, 0, W, H, fill=1, stroke=0)
            c.setFillColorRGB(1, 1, 1)
            c.setFont("Helvetica-Bold", 14)
            c.drawString(margin, H - margin + 2, "DASHBOARD EXECUTIVO")
            c.setFont("Helvetica", 9)
            c.setFillColorRGB(*muted_rgb)
            c.drawString(margin, H - margin - 14, f"DB: {os.path.basename(self.db_path)}  |  Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
            c.setFillColorRGB(1, 1, 1)

        def _draw_top_cards(y_top):
            card_h = 96
            for col in (0, 1):
                x = margin + col * (col_w + gap)
                c.setFillColorRGB(0.03, 0.15, 0.15)
                c.roundRect(x + 2, y_top - 2, col_w, card_h, 10, fill=1, stroke=0)
                c.setFillColorRGB(*card_rgb)
                c.roundRect(x, y_top, col_w, card_h, 10, fill=1, stroke=0)

            pct_conc = (cont_conc / cont_total) if cont_total else 0.0
            pct_sobra = (cont_sobra / cont_total) if cont_total else 0.0
            pct_q_conc = (cont_q_conc / cont_q_total) if cont_q_total else 0.0
            pct_q_sobra = (cont_q_sobra / cont_q_total) if cont_q_total else 0.0

            x0 = margin
            c.setFillColorRGB(1, 1, 1)
            c.setFont("Helvetica-Bold", 10)
            c.drawString(x0 + 12, y_top + card_h - 18, "Base Contábil — Residual")

            c.setFont("Helvetica-Bold", 9)
            c.drawString(x0 + 12, y_top + card_h - 36, "TOTAL")
            c.setFont("Helvetica", 9)
            c.drawRightString(x0 + col_w - 12, y_top + card_h - 36, f"{_br_money(cont_total)}   (100,00%)")

            c.setFont("Helvetica-Bold", 9)
            c.drawString(x0 + 12, y_top + card_h - 50, "Conciliados")
            c.setFont("Helvetica", 9)
            c.drawRightString(x0 + col_w - 12, y_top + card_h - 50, f"{_br_money(cont_conc)}   ({_br_pct(pct_conc)})")

            c.setFont("Helvetica-Bold", 9)
            c.drawString(x0 + 12, y_top + card_h - 64, "Sobras")
            c.setFont("Helvetica", 9)
            c.drawRightString(x0 + col_w - 12, y_top + card_h - 64, f"{_br_money(cont_sobra)}   ({_br_pct(pct_sobra)})")

            c.setFillColorRGB(*muted_rgb)
            c.setFont("Helvetica-Bold", 8.5)
            c.drawString(x0 + 12, y_top + card_h - 80, "ITENS")
            c.setFont("Helvetica", 8.5)
            c.drawRightString(x0 + col_w - 12, y_top + card_h - 80, f"{cont_q_total}   (100,00%)")

            c.setFont("Helvetica-Bold", 8.5)
            c.drawString(x0 + 12, y_top + card_h - 92, "Conciliados")
            c.setFont("Helvetica", 8.5)
            c.drawRightString(x0 + col_w - 12, y_top + card_h - 92, f"{cont_q_conc}   ({_br_pct(pct_q_conc)})")

            c.setFont("Helvetica-Bold", 8.5)
            c.drawString(x0 + 12, y_top + card_h - 104, "Sobras")
            c.setFont("Helvetica", 8.5)
            c.drawRightString(x0 + col_w - 12, y_top + card_h - 104, f"{cont_q_sobra}   ({_br_pct(pct_q_sobra)})")

            c.setFont("Helvetica", 7.5)
            c.setFillColorRGB(*muted_rgb)
            c.drawString(x0 + 12, y_top + 8, "(Conta: Ativo Imobilizado — usa COD_CONTA)")

            pct_conc_f = (fis_conc / fis_total) if fis_total else 0.0
            pct_sobra_f = (fis_sobra / fis_total) if fis_total else 0.0
            x1 = margin + (col_w + gap)
            c.setFillColorRGB(1, 1, 1)
            c.setFont("Helvetica-Bold", 10)
            c.drawString(x1 + 12, y_top + card_h - 18, "Base Física — Quantidade")

            c.setFont("Helvetica-Bold", 9)
            c.drawString(x1 + 12, y_top + card_h - 36, "TOTAL")
            c.setFont("Helvetica", 9)
            c.drawRightString(x1 + col_w - 12, y_top + card_h - 36, f"{fis_total}   (100,00%)")

            c.setFont("Helvetica-Bold", 9)
            c.drawString(x1 + 12, y_top + card_h - 50, "Conciliados")
            c.setFont("Helvetica", 9)
            c.drawRightString(x1 + col_w - 12, y_top + card_h - 50, f"{fis_conc}   ({_br_pct(pct_conc_f)})")

            c.setFont("Helvetica-Bold", 9)
            c.drawString(x1 + 12, y_top + card_h - 64, "Sobras")
            c.setFont("Helvetica", 9)
            c.drawRightString(x1 + col_w - 12, y_top + card_h - 64, f"{fis_sobra}   ({_br_pct(pct_sobra_f)})")

            c.setFont("Helvetica", 7.5)
            c.setFillColorRGB(*muted_rgb)
            c.drawString(x1 + 12, y_top + 8, "(Sem contas e sem valores — usa QTD)")
            return y_top - 14

        def _draw_tile(x, y, w, h, subtitle, img_path):
            c.setFillColorRGB(0.03, 0.15, 0.15)
            c.roundRect(x + 2, y - 2, w, h, 10, fill=1, stroke=0)
            c.setFillColorRGB(*card_rgb)
            c.roundRect(x, y, w, h, 10, fill=1, stroke=0)
            c.setFillColorRGB(1, 1, 1)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(x + 10, y + h - 16, (subtitle or "")[:90])
            try:
                c.drawImage(ImageReader(img_path), x + 8, y + 8, width=w - 16, height=h - 32, preserveAspectRatio=True, anchor="c")
            except Exception:
                c.setFont("Helvetica", 9)
                c.setFillColorRGB(*muted_rgb)
                c.drawString(x + 10, y + h / 2, "Falha ao renderizar gráfico")

        _draw_header()
        y = H - margin - 46
        y = _draw_top_cards(y - 96)
        tile_h = 170
        y -= 10

        col = 0
        for subtitle, img in chart_imgs:
            x = margin + col * (col_w + gap)
            _draw_tile(x, y - tile_h, col_w, tile_h, subtitle, img)
            col += 1
            if col >= 2:
                col = 0
                y -= (tile_h + 12)

            if y - tile_h < margin + 12 and (subtitle, img) != chart_imgs[-1]:
                c.showPage()
                _draw_header()
                y = H - margin - 46
                y = _draw_top_cards(y - 96)
                y -= 10
                col = 0

        c.showPage()
        c.save()

        try:
            for _, p in chart_imgs:
                if os.path.exists(p):
                    os.remove(p)
            for fn in os.listdir(tmp_dir):
                fp = os.path.join(tmp_dir, fn)
                if os.path.isfile(fp):
                    os.remove(fp)
            os.rmdir(tmp_dir)
        except Exception:
            pass

    def _gerar_pdf(self):
        if not self.db_path or not os.path.exists(self.db_path):
            messagebox.showwarning("PDF", "Banco de dados não encontrado.")
            return
        out = filedialog.asksaveasfilename(
            title="Salvar relatório PDF",
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf")],
            initialfile=f"relatorio_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
        )
        if not out:
            return
        try:
            self._export_pdf_print(out)
            messagebox.showinfo("PDF", f"Relatório gerado:\n{out}")
        except Exception as e:
            messagebox.showerror("PDF", f"Falha ao gerar PDF:\n{e}")

    def _on_close(self):
        try:
            for fig in self._figs:
                try:
                    plt.close(fig)
                except Exception:
                    pass
        finally:
            self.destroy()

# =========================================================
# DASHBOARD SINTÉTICO (8 GRÁFICOS EXECUTIVOS) - EVS
# =========================================================

class DashboardSintetico(tk.Toplevel):
    def __init__(self, master=None, db_path=None, *args, **kwargs):
        super().__init__(master)
        _maximize_window(self)
        self.title("Dashboard Sintético – EVS")
        self.geometry("1400x800")
        self.configure(bg=BG)

        if FigureCanvasTkAgg is None or Figure is None:
            messagebox.showerror(
                "Dashboard",
                "O módulo 'matplotlib' não está instalado neste Python.\n\n"
                "Instale com:\n"
                "/usr/local/bin/python3 -m pip install matplotlib\n\n"
                f"Erro: {_MATPLOTLIB_ERR}"
            )
            self.destroy()
            return

        self.db_path = db_path
        self.con = connect(self.db_path)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self._fig_bg = "#4a4a4a"
        self._ax_bg = "#4a4a4a"

        self._mode, self._tbl_contabil, self._tbl_conciliados = self._detect_norm_tables()
        if self._mode == "NORM":
            self._col_residual = self._detect_residual_col(self._tbl_contabil)
            self._col_id_contabil = self._detect_id_col(self._tbl_contabil)
            self._col_base_conc, self._col_id_conc = self._detect_conciliados_cols(self._tbl_conciliados)
        else:
            self._tbl_ctb, self._tbl_depara = self._detect_legacy_tables()
            self._col_residual = self._detect_residual_col(self._tbl_ctb)

        # Filtros (opcional): Filial / Centro de Custo
        # Colunas de filtro (default)
        self._col_filial, self._col_ccusto = None, None
        try:
            t0 = self._tbl_contabil if self._mode == "NORM" else self._tbl_ctb
            self._col_filial, self._col_ccusto = self._detect_filter_cols(t0)
        except Exception:
            self._col_filial, self._col_ccusto = None, None

        self.filial_var = tk.StringVar(value="(TODAS)")
        self.ccusto_var = tk.StringVar(value="(TODAS)")

        self.metricas = self._calc()
        self._charts = []

        self._layout()

        # Carrega valores de filtro (se colunas existirem na tabela contábil)
        try:
            t = self._tbl_contabil if self._mode == "NORM" else self._tbl_ctb
            self._col_filial, self._col_ccusto = self._detect_filter_cols(t)
            con0 = self._conn()
            if self._col_filial:
                filiais = ["(TODAS)"] + _distinct_values(con0, t, f'"{self._col_filial}"')
            else:
                filiais = ["(TODAS)"]
            if self._col_ccusto:
                ccustos = ["(TODAS)"] + _distinct_values(con0, t, f'"{self._col_ccusto}"')
            else:
                ccustos = ["(TODAS)"]
            try:
                self.cbo_filial["values"] = filiais
                self.cbo_ccusto["values"] = ccustos
            except Exception:
                pass
        except Exception:
            self._col_filial, self._col_ccusto = None, None

        self._draw()


    def _on_close(self):
        try:
            if getattr(self, "con", None) is not None:
                self.con.close()
        except Exception:
            pass
        self.destroy()

    def _layout(self):
        header = tk.Frame(self, bg=BG)
        header.pack(fill="x", padx=12, pady=(12, 0))

        tk.Label(header, text="DASHBOARD SINTÉTICO", bg=BG, fg="white", font=("Arial", 16, "bold")).pack(side="left")

        btns = tk.Frame(header, bg=BG)
        btns.pack(side="right")
        tk.Button(btns, text="GERAR PDF", width=14, command=self._on_export_pdf).pack(side="left", padx=(0, 8))
        tk.Button(btns, text="FECHAR", width=10, command=self._on_close).pack(side="left")

        # Barra de filtros (Filial / CCusto)
        filt = tk.Frame(self, bg=BG)
        filt.pack(fill="x", padx=12, pady=(8, 6))

        tk.Label(filt, text="Filial:", bg=BG, fg="white", font=("Arial", 10, "bold")).pack(side="left")
        self.cbo_filial = ttk.Combobox(filt, textvariable=self.filial_var, state="readonly", width=16)
        self.cbo_filial.pack(side="left", padx=(6, 12))

        tk.Label(filt, text="Centro de Custo:", bg=BG, fg="white", font=("Arial", 10, "bold")).pack(side="left")
        self.cbo_ccusto = ttk.Combobox(filt, textvariable=self.ccusto_var, state="readonly", width=20)
        self.cbo_ccusto.pack(side="left", padx=(6, 12))

        tk.Button(filt, text="Aplicar Filtro", width=14, command=self._refresh).pack(side="left", padx=6)
        tk.Button(filt, text="Limpar", width=10, command=lambda: (self.filial_var.set('(TODAS)'), self.ccusto_var.set('(TODAS)'), self._refresh())).pack(side="left", padx=6)

        self.grid_area = tk.Frame(self, bg=BG)
        self.grid_area.pack(fill="both", expand=True, padx=12, pady=12)

        self.frames = []
        for i in range(8):
            f = tk.Frame(self.grid_area, bg=CARD_BG, highlightbackground="#2b8a8a", highlightthickness=1)
            f.grid(row=i // 3, column=i % 3, padx=12, pady=12, sticky="nsew")
            self.frames.append(f)

        for i in range(3):
            self.grid_area.columnconfigure(i, weight=1)
            self.grid_area.rowconfigure(i, weight=1)

    def _on_export_pdf(self):
        out = filedialog.asksaveasfilename(
            title="Salvar PDF do Dashboard Sintético",
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf")],
            initialfile=f"dashboard_sintetico_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf",
        )
        if not out:
            return
        try:
            self._export_pdf_print(out)
            messagebox.showinfo("PDF", f"PDF gerado:\n{out}")
        except Exception as e:
            messagebox.showerror("PDF", f"Falha ao gerar PDF:\n{e}")

    # ---------- DB helpers ----------
    def _conn(self):
        return connect(self.db_path)

    def _list_tables(self):
        with self._conn() as c:
            rows = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return [r[0] for r in rows]

    def _table_cols(self, table):
        with self._conn() as c:
            rows = c.execute(f'PRAGMA table_info("{table}")').fetchall()
        return [r[1] for r in rows]

    def _detect_norm_tables(self):
        tables = self._list_tables()
        low = {t.lower(): t for t in tables}
        if "contabil" in low and "conciliados" in low:
            return "NORM", low["contabil"], low["conciliados"]
        return "LEGACY", None, None

    def _detect_legacy_tables(self):
        tables = self._list_tables()
        low = {t.lower(): t for t in tables}

        def pick(exact_candidates, contains_any=None):
            for cand in exact_candidates:
                if cand.lower() in low:
                    return low[cand.lower()]
            if contains_any:
                for t in tables:
                    tl = t.lower()
                    if any(k in tl for k in contains_any):
                        return t
            return None

        tbl_ctb = pick(["BsContabil", "bscontabil"], contains_any=["contabil", "ctb"])
        tbl_depara = pick(["BsDePara", "BsDepara", "bsdepara"], contains_any=["depara", "concili"])

        if not tbl_ctb:
            raise RuntimeError(f"Não encontrei tabela Contábil no DB. Tabelas: {tables}")
        if not tbl_depara:
            raise RuntimeError(f"Não encontrei tabela De-Para no DB. Tabelas: {tables}")
        return tbl_ctb, tbl_depara

    def _detect_residual_col(self, table):
        cols = self._table_cols(table)
        if not cols:
            raise RuntimeError(f"Tabela '{table}' não possui colunas (PRAGMA vazio).")

        preferred = ["VLR_RESIDUAL", "VLR. RESIDUAL", "VLR RESIDUAL", "VALOR_RESIDUAL", "VALOR RESIDUAL", "RESIDUAL"]
        cols_lower = {c.lower(): c for c in cols}
        for p in preferred:
            if p.lower() in cols_lower:
                return cols_lower[p.lower()]
        for c in cols:
            if "resid" in c.lower():
                return c
        raise RuntimeError(f"Não encontrei coluna de residual na tabela {table}. Colunas: {cols}")

    def _detect_id_col(self, table):
        cols = self._table_cols(table)
        cols_lower = {c.lower(): c for c in cols}
        if "id" in cols_lower:
            return cols_lower["id"]
        for c in cols:
            cl = c.lower()
            if cl == "id" or cl.endswith("_id") or cl.startswith("id"):
                return c
        raise RuntimeError(f"Não encontrei coluna ID na tabela {table}. Colunas: {cols}")

    def _detect_conciliados_cols(self, table):
        cols = self._table_cols(table)
        if not cols:
            raise RuntimeError(f"Tabela '{table}' não possui colunas.")
        cols_lower = {c.lower(): c for c in cols}
        base_col = cols_lower.get("base")
        id_col = cols_lower.get("id")
        if not base_col:
            for c in cols:
                if "base" in c.lower():
                    base_col = c
                    break
        if not id_col:
            for c in cols:
                cl = c.lower()
                if cl == "id" or cl.endswith("_id") or cl.startswith("id"):
                    id_col = c
                    break
        if not base_col or not id_col:
            raise RuntimeError(f"Não encontrei colunas BASE/ID em '{table}'. Colunas: {cols}")
        return base_col, id_col

    def _refresh(self):
            """Recalcula e redesenha com filtros atuais."""
            try:
                metricas_new = self._calc()
            except Exception as e:
                messagebox.showerror("Dashboard", f"Erro ao aplicar filtro no Dashboard Sintético:\n\n{e}")
                return
    
            # Só limpa/redesenha se o cálculo passou
            self.metricas = metricas_new
            for fr in getattr(self, "frames", []):
                try:
                    for w in list(fr.winfo_children()):
                        w.destroy()
                except Exception:
                    pass
    
            try:
                self._draw()
            except Exception as e:
                messagebox.showerror("Dashboard", f"Erro ao redesenhar gráficos (Sintético):\n\n{e}")
                # não destrói a janela
                return
    
        
    def _detect_filter_cols(self, table: str) -> tuple[str | None, str | None]:
        cols = self._table_cols(table)
        if not cols:
            return None, None
        cols_lower = {c.lower(): c for c in cols}
        filial = cols_lower.get("filial")
        ccusto = cols_lower.get("ccusto")
        return filial, ccusto

    def _where_filters_tbl(self, alias: str, col_filial: str | None, col_ccusto: str | None) -> tuple[str, list]:
        """Monta WHERE (com alias) para filtros de Filial/CCusto (aceita alfanumérico)."""
        conds = []
        params: list = []
        filial = self.filial_var.get() if hasattr(self, "filial_var") else "(TODAS)"
        ccusto = self.ccusto_var.get() if hasattr(self, "ccusto_var") else "(TODAS)"

        if col_filial and filial and filial != "(TODAS)":
            conds.append(f'TRIM(CAST({alias}."{col_filial}" AS TEXT)) = TRIM(?)')
            params.append(filial)
        if col_ccusto and ccusto and ccusto != "(TODAS)":
            conds.append(f'TRIM(CAST({alias}."{col_ccusto}" AS TEXT)) = TRIM(?)')
            params.append(ccusto)

        if not conds:
            return "", []
        return " WHERE " + " AND ".join(conds), params

    def _and_filters_tbl(self, alias: str, col_filial: str | None, col_ccusto: str | None) -> tuple[str, list]:
        """Retorna ' AND ...' + params (para anexar em WHERE já existente)."""
        conds = []
        params: list = []
        filial = self.filial_var.get() if hasattr(self, "filial_var") else "(TODAS)"
        ccusto = self.ccusto_var.get() if hasattr(self, "ccusto_var") else "(TODAS)"

        if col_filial and filial and filial != "(TODAS)":
            conds.append(f'TRIM(CAST({alias}."{col_filial}" AS TEXT)) = TRIM(?)')
            params.append(filial)
        if col_ccusto and ccusto and ccusto != "(TODAS)":
            conds.append(f'TRIM(CAST({alias}."{col_ccusto}" AS TEXT)) = TRIM(?)')
            params.append(ccusto)

        if not conds:
            return "", []
        return " AND " + " AND ".join(conds), params

    def _apply_sql_filters(self, sql: str, params: tuple | list | None):
        """Aplica filtros de Filial/CCusto automaticamente quando a query usa a tabela contábil."""
        params_list = list(params) if params else []
        filial = self.filial_var.get()
        ccusto = self.ccusto_var.get()
        if (not filial or filial == "(TODAS)") and (not ccusto or ccusto == "(TODAS)"):
            return sql, tuple(params_list)

        # tabela alvo contábil depende do modo
        t = getattr(self, "_tbl_contabil", None) or getattr(self, "_tbl_ctb", None)
        if not t:
            return sql, tuple(params_list)

        col_filial = getattr(self, "_col_filial", None)
        col_ccusto = getattr(self, "_col_ccusto", None)
        if not col_filial and not col_ccusto:
            return sql, tuple(params_list)

        # Detecta alias após FROM "tabela"
        m = re.search(rf'FROM\s+"{re.escape(t)}"(?:\s+(\w+))?', sql, flags=re.I)
        if not m:
            return sql, tuple(params_list)
        alias = m.group(1) or ""  # sem alias
        prefix = f'{alias}.' if alias else ''
        conds = []
        if col_filial and filial and filial != "(TODAS)":
            conds.append(f'TRIM(CAST({prefix}"{col_filial}" AS TEXT)) = TRIM(?)')
            params_list.append(filial)
        if col_ccusto and ccusto and ccusto != "(TODAS)":
            conds.append(f'TRIM(CAST({prefix}"{col_ccusto}" AS TEXT)) = TRIM(?)')
            params_list.append(ccusto)

        if not conds:
            return sql, tuple(params_list)

        if re.search(r'\bWHERE\b', sql, flags=re.I):
            sql = re.sub(r'\bWHERE\b', 'WHERE', sql, count=1, flags=re.I) + " AND " + " AND ".join(conds)
        else:
            sql = sql + " WHERE " + " AND ".join(conds)
        return sql, tuple(params_list)


    def _q1(self, q, params=()):
        with self._conn() as c:
            row = c.execute(q, params).fetchone()
            v = row[0] if row else None
            return float(v) if v is not None else 0.0

    def _qi(self, q, params=()):
        with self._conn() as c:
            row = c.execute(q, params).fetchone()
            v = row[0] if row else None
            return int(v) if v is not None else 0

    # ---------- formatting ----------
    def _fmt_money(self, v):
        s = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"

    def _fmt_int(self, v):
        return f"{int(v):,}".replace(",", ".")

    # ---------- metrics ----------
    def _calc(self):
        if self._mode == "NORM":
            col = self._col_residual
            t_ctb = self._tbl_contabil
            t_conc = self._tbl_conciliados
            col_base, col_idc = self._col_base_conc, self._col_id_conc
            col_id_ctb = self._col_id_contabil

            and_filt, p_filt = self._and_filters_tbl("t", self._col_filial, self._col_ccusto)

            # Totais (aplicando filtro em contabil)
            tot_val = self._q1(
                f'SELECT SUM(COALESCE(t."{col}",0)) FROM "{t_ctb}" t WHERE 1=1' + and_filt,
                tuple(p_filt),
            )
            tot_it = self._qi(
                f'SELECT COUNT(*) FROM "{t_ctb}" t WHERE 1=1' + and_filt,
                tuple(p_filt),
            )

            # Conciliados (aplicando filtro em contabil via EXISTS)
            q_val = (
                f'SELECT SUM(COALESCE(t."{col}",0)) '
                f'FROM "{t_ctb}" t '
                f'WHERE EXISTS ('
                f'  SELECT 1 FROM "{t_conc}" c '
                f'  WHERE c."{col_base}" = ? AND c."{col_idc}" = t."{col_id_ctb}"'
                f')' + and_filt
            )
            conc_val = self._q1(q_val, ("CTB", *p_filt))

            q_it = (
                f'SELECT COUNT(*) '
                f'FROM "{t_ctb}" t '
                f'WHERE EXISTS ('
                f'  SELECT 1 FROM "{t_conc}" c '
                f'  WHERE c."{col_base}" = ? AND c."{col_idc}" = t."{col_id_ctb}"'
                f')' + and_filt
            )
            conc_it = self._qi(q_it, ("CTB", *p_filt))
        else:
            col = self._col_residual
            t_ctb = self._tbl_ctb
            t_dep = self._tbl_depara

            tot_val = self._q1(f'SELECT SUM(COALESCE("{col}",0)) FROM "{t_ctb}"')
            tot_it = self._qi(f'SELECT COUNT(*) FROM "{t_ctb}"')

            dep_cols = self._table_cols(t_dep)
            dep_res_col = None
            for c in dep_cols:
                if c.lower() == col.lower() or ("resid" in c.lower()):
                    dep_res_col = c
                    break
            conc_val = self._q1(f'SELECT SUM(COALESCE("{dep_res_col}",0)) FROM "{t_dep}"') if dep_res_col else 0.0
            conc_it = self._qi(f'SELECT COUNT(*) FROM "{t_dep}"')

        sobra_val = max(tot_val - conc_val, 0)
        sobra_it = max(tot_it - conc_it, 0)

        pct_val_c = (conc_val / tot_val * 100) if tot_val else 0
        pct_val_s = (sobra_val / tot_val * 100) if tot_val else 0
        pct_it_c = (conc_it / tot_it * 100) if tot_it else 0
        pct_it_s = (sobra_it / tot_it * 100) if tot_it else 0

        return dict(
            total_valor=tot_val,
            total_itens=tot_it,
            valor_conc=conc_val,
            valor_sobra=sobra_val,
            itens_conc=conc_it,
            itens_sobra=sobra_it,
            pct_val_c=pct_val_c,
            pct_val_s=pct_val_s,
            pct_it_c=pct_it_c,
            pct_it_s=pct_it_s,
        )

    def _calc_fisico_totais(self) -> dict:
        """Totais da base física (Total / Conciliados / Sobras).

        Importante: no modo 'NORM' (padrão), considera conciliado quando existe registro
        na tabela `conciliados` com BASE='FIS' para o ID do físico.
        """
        cur = self.con.cursor()

        # --- filtros (fisico) ---
        f_conds = []
        f_params = []
        filial = self.filial_var.get() if hasattr(self, "filial_var") else "(TODAS)"
        ccusto = self.ccusto_var.get() if hasattr(self, "ccusto_var") else "(TODAS)"

        if filial and filial != "(TODAS)":
            f_conds.append("TRIM(CAST(f.FILIAL AS TEXT)) = TRIM(?)")
            f_params.append(filial)
        if ccusto and ccusto != "(TODAS)":
            f_conds.append("TRIM(CAST(f.CCUSTO AS TEXT)) = TRIM(?)")
            f_params.append(ccusto)

        where_f = (" WHERE " + " AND ".join(f_conds)) if f_conds else ""

        # Total (com filtro)
        total = cur.execute("SELECT COUNT(*) FROM fisico f" + where_f, f_params).fetchone()[0] or 0

        # Conciliados
        if self._mode == "NORM":
            # EVITA 'WHERE ... WHERE ...' quando há filtro:
            # monta todas as condições numa única cláusula WHERE.
            conds = ["EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='FIS' AND c.ID=f.ID)"]
            conds += f_conds
            where_conc = " WHERE " + " AND ".join(conds)
            conc = cur.execute("SELECT COUNT(*) FROM fisico f" + where_conc, f_params).fetchone()[0] or 0
        else:
            # modo alternativo: considera conciliado quando existe no De-Para (ID_FISICO != 0)
            extra = (" AND " + " AND ".join(f_conds)) if f_conds else ""
            conc = cur.execute(
                "SELECT COUNT(DISTINCT d.ID_FISICO) "
                "FROM depara d JOIN fisico f ON f.ID=d.ID_FISICO "
                "WHERE COALESCE(d.ID_FISICO,0) <> 0" + extra,
                f_params,
            ).fetchone()[0] or 0

        sobras = int(total) - int(conc)
        if sobras < 0:
            sobras = 0

        return {"total": int(total), "conc": int(conc), "sobras": int(sobras)}

    # ---------- plotting helpers ----------
    def _mk_fig(self):
        fig = Figure(figsize=(4.3, 3.0), dpi=110)
        fig.patch.set_facecolor(self._fig_bg)
        ax = fig.add_subplot(111)
        ax.set_facecolor(self._ax_bg)
        return fig, ax

    def _hide_axes(self, ax):
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_visible(False)

    def _donut_total(self, frame, title, center_text, value_text):
        fig, ax = self._mk_fig()
        ax.pie([1], labels=[""], startangle=90, wedgeprops=dict(width=0.35))
        ax.text(0, 0.08, center_text, ha="center", va="center", color="white", fontsize=14, fontweight="bold")
        ax.text(0, -0.12, value_text, ha="center", va="center", color="white", fontsize=12)
        ax.set_title(title, color="white", fontsize=12, fontweight="bold", pad=10)
        self._hide_axes(ax)

        self._charts.append((title, fig))
        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

    def _pie_exec(self, frame, title, labels, values, value_formatter=None):
        fig, ax = self._mk_fig()

        def autopct(pct):
            total = sum(values) if values else 0
            val = pct * total / 100.0 if total else 0
            if value_formatter:
                return f"{pct:.1f}%\n{value_formatter(val)}"
            return f"{pct:.1f}%"

        ax.pie(values, labels=labels, autopct=autopct, startangle=90)
        ax.set_title(title, color="white", fontsize=12, fontweight="bold", pad=10)
        self._hide_axes(ax)

        self._charts.append((title, fig))
        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

    def _bar_pct(self, frame, title, labels, values):
        fig, ax = self._mk_fig()
        bars = ax.bar(labels, values)
        ax.set_ylim(0, 100)
        ax.set_title(title, color="white", fontsize=12, fontweight="bold", pad=10)
        self._hide_axes(ax)
        for b, v in zip(bars, values):
            ax.text(b.get_x() + b.get_width() / 2, v + 1, f"{v:.1f}%", ha="center", va="bottom", color="white", fontsize=11)

        self._charts.append((title, fig))
        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

    def _bar_itens(self, frame, title: str, labels, values):
        fig, ax = self._mk_fig()
        bars = ax.bar(list(labels), list(values))
        ax.set_title(title, color="white", fontsize=12, fontweight="bold", pad=10)
        self._hide_axes(ax)
        for b, v in zip(bars, values):
            ax.text(b.get_x() + b.get_width() / 2, v, f"{int(v)}", ha="center", va="bottom", color="white", fontsize=11)

        self._charts.append((title, fig))
        canvas = FigureCanvasTkAgg(fig, master=frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

    def _draw(self):
        self._charts = []
        m = self.metricas
        f = self._calc_fisico_totais()

        self._donut_total(self.frames[0], "Total da Base Contábil R$", "TOTAL", self._fmt_money(m["total_valor"]))
        self._donut_total(self.frames[1], "Total da Base Contábil Itens", "ITENS", self._fmt_int(m["total_itens"]))

        self._pie_exec(self.frames[2], "Base Contábil Conciliada R$", ["CONCILIADO", "SOBRAS"], [m["valor_conc"], m["valor_sobra"]], value_formatter=self._fmt_money)
        self._bar_pct(self.frames[3], "Base Contábil Conciliada %", ["CONCILIADO", "SOBRAS"], [m["pct_val_c"], m["pct_val_s"]])

        self._pie_exec(self.frames[4], "Base Contábil Conciliada Itens", ["CONCILIADO", "SOBRAS"], [m["itens_conc"], m["itens_sobra"]], value_formatter=lambda v: self._fmt_int(round(v)))
        self._bar_pct(self.frames[5], "Base Contábil Conciliada Itens %", ["CONCILIADO", "SOBRAS"], [m["pct_it_c"], m["pct_it_s"]])

        self._bar_itens(self.frames[6], "Base Física — Itens (Total / Conciliados / Sobras)", ["TOTAL", "CONCILIADOS", "SOBRAS"], [f["total"], f["conc"], f["sobras"]])
        pct_f_c = (100.0 * f["conc"] / f["total"]) if f["total"] else 0.0
        pct_f_s = (100.0 * f["sobras"] / f["total"]) if f["total"] else 0.0
        self._bar_pct(self.frames[7], "Base Física — % (Conciliados x Sobras)", ["CONCILIADOS", "SOBRAS"], [pct_f_c, pct_f_s])

    def _export_pdf_print(self, output_pdf: str):
        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.pdfgen import canvas as pdf_canvas
            from reportlab.lib.utils import ImageReader
        except Exception as e:
            raise RuntimeError(f"reportlab não disponível: {e}")

        if not self._charts:
            self._draw()

        tmp_dir = os.path.join(os.path.dirname(output_pdf) or ".", "_tmp_dashboard_sint_pdf")
        os.makedirs(tmp_dir, exist_ok=True)

        imgs = []
        for idx, (title, fig) in enumerate(self._charts, start=1):
            p = os.path.join(tmp_dir, f"sint_{idx:02d}.png")
            fig.savefig(p, dpi=170, bbox_inches="tight")
            imgs.append((title, p))

        page_w, page_h = landscape(A4)
        c = pdf_canvas.Canvas(output_pdf, pagesize=landscape(A4))

        c.setFillColorRGB(0.95, 0.95, 0.95)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(24, page_h - 28, "Dashboard Sintético — EVS")

        margin_x = 24
        margin_top = 52
        margin_bottom = 24
        gap_x = 18
        gap_y = 18

        grid_w = page_w - 2 * margin_x
        grid_h = page_h - margin_top - margin_bottom

        cell_w = (grid_w - 3 * gap_x) / 4
        cell_h = (grid_h - 1 * gap_y) / 2

        def cell_xy(i):
            r = i // 4
            col = i % 4
            x = margin_x + col * (cell_w + gap_x)
            y = page_h - margin_top - (r + 1) * cell_h - r * gap_y
            return x, y

        for i, (_, p) in enumerate(imgs[:8]):
            x, y = cell_xy(i)
            img = ImageReader(p)
            c.drawImage(img, x, y, width=cell_w, height=cell_h, preserveAspectRatio=True, anchor="c")

        c.showPage()
        c.save()

        try:
            for _, p in imgs:
                if os.path.exists(p):
                    os.remove(p)
            for fn in os.listdir(tmp_dir):
                fp = os.path.join(tmp_dir, fn)
                if os.path.isfile(fp):
                    os.remove(fp)
            os.rmdir(tmp_dir)
        except Exception:
            pass

__all__ = ["DashboardWindow", "DashboardSintetico"]
