# interface_inicial_v2_dashboard_fixed.py
from __future__ import annotations

import math
import os
import shutil
import threading
import tempfile
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

from importer_v2 import import_bases
from run_auto_v2 import main as run_auto_main
from exporter_v2 import export_bsdepara
from manual_v2_FINAL import ManualV2Window
from dashboard_v2 import DashboardWindow
from depara_import import DeParaImportWindow
from db_utils_v2 import connect

def _default_db_path(base_dir: str) -> str:
    """Usa conciliador.db como padrão; se não existir, cai para conciliador_v2.db (compatibilidade)."""
    p1 = os.path.join(base_dir, "conciliador.db")
    p2 = os.path.join(base_dir, "conciliador_v2.db")
    if os.path.exists(p1):
        return p1
    if os.path.exists(p2):
        return p2
    return p1


BG = "#225781"

class TelaInicialV2(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("EVS_COTEJ_v3")
        self.configure(bg=BG)
        self.screen_w = self.winfo_screenwidth()
        self.screen_h = self.winfo_screenheight()
        self.compact_ui = self.screen_h <= 800

        # Vars
        # Arquivos do sistema (db e samples) ficam na mesma pasta deste módulo.
        self.base_dir = os.path.abspath(os.path.dirname(__file__))
        samples_dir = os.path.join(self.base_dir, "samples")

        self.var_fis = tk.StringVar(value=os.path.join(samples_dir, "BsFisico.xlsx"))
        self.var_ctb = tk.StringVar(value=os.path.join(samples_dir, "BsContabil.xlsx"))
        self.var_tpl = tk.StringVar(value=os.path.join(samples_dir, "BsDePara.xlsx"))
        self.var_out = tk.StringVar(value=os.path.join(samples_dir, "BsDePara_conciliados.xlsx"))
        self.var_db = tk.StringVar(value=_default_db_path(self.base_dir))

        # Métricas (mini-dashboard)
        self.mv_fis_total = tk.StringVar(value="0")
        self.mv_ctb_total = tk.StringVar(value="0")
        self.mv_pares = tk.StringVar(value="0")
        self.mv_fis_conc = tk.StringVar(value="0")
        self.mv_ctb_conc = tk.StringVar(value="0")
        self.mv_sobras_fis = tk.StringVar(value="0")
        self.mv_sobras_ctb = tk.StringVar(value="0")
        self.mv_resumo = tk.StringVar(value="")
        self.mv_backend = tk.StringVar(value="Banco ativo: -")
        self.mv_line_import = tk.StringVar(value="")
        self.mv_line_conc = tk.StringVar(value="")
        self.mv_line_sobra = tk.StringVar(value="")
        self.logo_image = None


        self._build()
        self._build_menu()

        self._update_dashboard()

        # Abre em tela maximizada no Windows para evitar corte por bordas/barra de tarefas.
        self.update_idletasks()
        if os.name == "nt":
            try:
                self.state("zoomed")
            except Exception:
                self.geometry(f"{self.screen_w}x{self.screen_h}+0+0")
        else:
            self.geometry(f"{self.screen_w}x{self.screen_h}+0+0")
        self.protocol("WM_DELETE_WINDOW", self._exit_system)

    # ---------- UI ----------
    def _build_menu(self) -> None:
        menubar = tk.Menu(self)

        menu_cfg = tk.Menu(menubar, tearoff=0)
        menu_cfg.add_command(label="Selecionar BsFisico.xlsx", command=self._pick_fis)
        menu_cfg.add_command(label="Selecionar BsContabil.xlsx", command=self._pick_ctb)
        menu_cfg.add_command(label="Selecionar Template BsDePara.xlsx", command=self._pick_tpl)
        menu_cfg.add_command(label="Selecionar Saída BsDePara.xlsx", command=self._pick_out)
        menu_cfg.add_command(label="Selecionar SQLite (.db)", command=self._pick_db)
        menubar.add_cascade(label="Configurações", menu=menu_cfg)

        menu_conc = tk.Menu(menubar, tearoff=0)
        menu_conc.add_command(label="Importar Bases (Excel → DB)", command=self._import_bases)
        menu_conc.add_command(label="Processar Automático (01)", command=self._processar_auto)
        menu_conc.add_command(label="Abrir Manual (pendentes)", command=self._abrir_manual)
        menu_conc.add_command(label="Exportar BsDePara", command=self._exportar_bsdepara)
        menu_conc.add_separator()
        menu_conc.add_command(label="Importar De-Para (direto)", command=self._importar_depara)
        menu_conc.add_command(label="Descotejar", command=self._abrir_descotejar)
        menu_conc.add_separator()
        menu_conc.add_command(label="Atualizar Informações", command=self._update_dashboard)
        menubar.add_cascade(label="Conciliação", menu=menu_conc)

        menu_rel = tk.Menu(menubar, tearoff=0)
        menu_rel.add_command(label="Dashboard / Relatórios", command=self._abrir_dashboard)
        menu_rel.add_command(label="Dashboard Sintético", command=self._abrir_dashboard_sintetico)
        menu_rel.add_command(label="Relatório Analítico (PDF)", command=self._abrir_relatorio_analitico)
        menubar.add_cascade(label="Relatório", menu=menu_rel)

        menu_sair = tk.Menu(menubar, tearoff=0)
        menu_sair.add_command(label="Sair", command=self._exit_system)
        menubar.add_cascade(label="Sair", menu=menu_sair)

        self.config(menu=menubar)

    def _build(self) -> None:
        pad = {"padx": 10, "pady": 5 if self.compact_ui else 8}
        title_font = ("Helvetica", 16 if self.compact_ui else 18, "bold")
        company_font = ("Helvetica", 10 if self.compact_ui else 12, "bold")
        row_font = ("Helvetica", 10 if self.compact_ui else 12, "bold")
        row_entry_w = 86 if self.compact_ui else 95
        btn_w = 14 if self.compact_ui else 16
        logo_pady = (16, 16) if self.compact_ui else (40, 40)
        dash_h = 140 if self.compact_ui else 190
        metric_font = ("Helvetica", 10 if self.compact_ui else 11, "bold")

        title = tk.Label(self, text="EVS_COTEJ_v3",
                         bg=BG, fg="white", font=title_font)
        title.grid(row=0, column=0, columnspan=2, sticky="w", **pad)

        company = tk.Label(
            self,
            text="EVS Consultoria Patrimonial e Avaliações Ltda.",
            bg=BG,
            fg="white",
            font=company_font,
        )
        company.grid(row=0, column=2, columnspan=2, sticky="e", **pad)

        # Row builder
        def row_file(r: int, label: str, var: tk.StringVar, pick_fn, save: bool = False):
            tk.Label(self, text=label, bg=BG, fg="white", font=row_font).grid(
                row=r, column=0, sticky="e", **pad
            )
            ent = tk.Entry(self, textvariable=var, width=row_entry_w, bg="#1a1a1a", fg="white", insertbackground="white")
            ent.grid(row=r, column=1, columnspan=2, sticky="ew", **pad)
            tk.Button(self, text="Selecionar", width=btn_w, command=pick_fn).grid(row=r, column=3, sticky="e", **pad)

        self.grid_columnconfigure(1, weight=1)
        self.grid_columnconfigure(2, weight=1)

        row_file(1, "BsFisico.xlsx:", self.var_fis, self._pick_fis)
        row_file(2, "BsContabil.xlsx:", self.var_ctb, self._pick_ctb)
        row_file(3, "Template BsDePara.xlsx:", self.var_tpl, self._pick_tpl)
        row_file(4, "Saída BsDePara.xlsx:", self.var_out, self._pick_out)
        row_file(5, "SQLite (db):", self.var_db, self._pick_db)

        # Guarda botões de ação (agora acionados via menu superior)
        self._action_buttons = []

        # Área do logo no lugar dos botões centrais.
        logo_frame = tk.Frame(self, bg=BG)
        logo_frame.grid(row=6, column=0, columnspan=4, pady=logo_pady, sticky="nsew")
        self._build_logo(logo_frame)

        # Mini-dashboard (contadores) — resumo em 3 linhas
        dash = tk.Frame(self, bg=BG)
        dash.grid(row=7, column=0, columnspan=4, sticky="nsew", padx=12, pady=(10, 6))

        # Área com altura fixa para manter o layout estável na tela principal
        self.grid_rowconfigure(7, weight=1, minsize=dash_h)
        dash.grid_propagate(False)
        dash.configure(height=dash_h)

        # Container central (centraliza o card e o botão)
        center = tk.Frame(dash, bg=BG)
        center.place(relx=0.5, rely=0.5, anchor="center")

        # Resumo em 3 linhas, centralizado e sem card visível.
        tk.Label(
            center,
            textvariable=self.mv_backend,
            bg=BG,
            fg="#cfe8e8",
            font=metric_font,
            justify="center",
        ).pack(anchor="center", padx=16, pady=(0, 8))
        tk.Label(
            center,
            textvariable=self.mv_line_import,
            bg=BG,
            fg="white",
            font=metric_font,
            justify="center",
        ).pack(anchor="center", padx=16, pady=(0, 2))
        tk.Label(
            center,
            textvariable=self.mv_line_conc,
            bg=BG,
            fg="white",
            font=metric_font,
            justify="center",
        ).pack(anchor="center", padx=16, pady=2)
        tk.Label(
            center,
            textvariable=self.mv_line_sobra,
            bg=BG,
            fg="white",
            font=metric_font,
            justify="center",
        ).pack(anchor="center", padx=16, pady=(2, 8 if self.compact_ui else 14))


        self.lbl_status = tk.Label(self, text="", bg=BG, fg="white", anchor="w")
        self.lbl_status.grid(row=8, column=0, columnspan=4, sticky="ew", padx=12, pady=10)

    def _build_logo(self, parent: tk.Widget) -> None:
        # Carrega logo de nomes padrão no diretório principal.
        candidates = [
            os.path.join(self.base_dir, "logo.png"),
            os.path.join(self.base_dir, "Logo.png"),
            os.path.join(self.base_dir, "logo.gif"),
            os.path.join(self.base_dir, "logo.ppm"),
            os.path.join(self.base_dir, "logo.pgm"),
        ]
        logo_path = next((p for p in candidates if os.path.isfile(p)), None)
        if not logo_path:
            tk.Label(
                parent,
                text="Logo não encontrado (adicione 'logo.png' na pasta do sistema).",
                bg=BG,
                fg="white",
                font=("Helvetica", 11, "bold"),
            ).pack()
            return

        try:
            img = tk.PhotoImage(file=logo_path)
            # Exibe o logo em formato mais retangular (mais largo e menos alto).
            max_w = 760 if self.compact_ui else 980
            x_factor = max(1, int(math.ceil(img.width() / max_w)))
            target_h = max(1, int(img.height() * (0.28 if self.compact_ui else 0.45)))
            y_factor = max(1, int(math.ceil(img.height() / target_h)))
            img = img.subsample(x_factor, y_factor)
            self.logo_image = img
            tk.Label(parent, image=self.logo_image, bg=BG, borderwidth=0, highlightthickness=0).pack()
        except Exception as e:
            tk.Label(
                parent,
                text=f"Falha ao carregar logo: {e}",
                bg=BG,
                fg="white",
                font=("Helvetica", 11, "bold"),
            ).pack()


    def _abrir_descotejar(self) -> None:
        """Abre a tela de DESCOTEJAR (desfazer conciliações) via importação de planilha De-Para."""
        db_path = self.var_db.get().strip()
        if not db_path:
            messagebox.showwarning(
                "Descotejar",
                "Informe o caminho do banco antes de descotejar.",
            )
            return
        if not self._can_connect_db(db_path):
            messagebox.showwarning("Descotejar", f"Não foi possível conectar no banco:\n{db_path}")
            return

        try:
            # Import tardio para não quebrar a tela inicial se o módulo ainda não estiver na pasta.
            from descotejar_import import DescotejarImportWindow  # type: ignore
        except Exception as e:
            messagebox.showerror(
                "Descotejar",
                "Não foi possível carregar o módulo de Descotejar.\n\n""Verifique se o arquivo 'descotejar_import.py' está na mesma pasta do sistema.\n\n"f"Detalhes: {e}",
            )
            return

        try:
            DescotejarImportWindow(self, db_path)
        except Exception as e:
            messagebox.showerror("Descotejar", f"Falha ao abrir a tela de Descotejar:\n{e}")

    def _abrir_relatorio_analitico(self) -> None:
        dbp = self.var_db.get().strip()
        if not dbp:
            messagebox.showerror("Relatório Analítico", "Selecione o banco.")
            return
        if not self._can_connect_db(dbp):
            messagebox.showerror("Relatório Analítico", f"Não foi possível conectar no banco:\n{dbp}")
            return

        win = tk.Toplevel(self)
        win.title("Relatório Analítico da Conciliação")
        win.configure(bg=BG)
        win.resizable(False, False)
        win.transient(self)
        win.grab_set()

        frm = tk.Frame(win, bg=BG)
        frm.pack(padx=16, pady=16)

        var_filial = tk.StringVar(value="(TODOS)")
        var_ccusto = tk.StringVar(value="(TODOS)")
        var_local = tk.StringVar(value="(TODOS)")

        vals_filial = self._distinct_filter_values(dbp, "FILIAL")
        vals_ccusto = self._distinct_filter_values(dbp, "CCUSTO")
        vals_local = self._distinct_filter_values(dbp, "LOCAL")

        tk.Label(frm, text="Filial:", bg=BG, fg="white", font=("Helvetica", 11, "bold")).grid(row=0, column=0, sticky="e", padx=8, pady=6)
        ttk.Combobox(frm, textvariable=var_filial, width=26, state="readonly", values=vals_filial).grid(row=0, column=1, sticky="w", padx=8, pady=6)
        tk.Label(frm, text="CCusto:", bg=BG, fg="white", font=("Helvetica", 11, "bold")).grid(row=1, column=0, sticky="e", padx=8, pady=6)
        ttk.Combobox(frm, textvariable=var_ccusto, width=26, state="readonly", values=vals_ccusto).grid(row=1, column=1, sticky="w", padx=8, pady=6)
        tk.Label(frm, text="Local:", bg=BG, fg="white", font=("Helvetica", 11, "bold")).grid(row=2, column=0, sticky="e", padx=8, pady=6)
        ttk.Combobox(frm, textvariable=var_local, width=26, state="readonly", values=vals_local).grid(row=2, column=1, sticky="w", padx=8, pady=6)

        def gerar():
            filial = var_filial.get().strip()
            ccusto = var_ccusto.get().strip()
            local = var_local.get().strip()

            outp = filedialog.asksaveasfilename(
                title="Salvar Relatório Analítico (PDF)",
                defaultextension=".pdf",
                filetypes=[("PDF", "*.pdf")],
                parent=win,
            )
            if not outp:
                return
            try:
                self._gerar_relatorio_analitico_pdf(dbp, outp, filial=filial, ccusto=ccusto, local=local)
                messagebox.showinfo("Relatório Analítico", f"PDF gerado com sucesso:\n{outp}", parent=win)
                win.destroy()
            except Exception as e:
                messagebox.showerror("Relatório Analítico", f"Falha ao gerar PDF:\n{e}", parent=win)

        btns = tk.Frame(frm, bg=BG)
        btns.grid(row=3, column=0, columnspan=2, pady=(10, 0))
        tk.Button(btns, text="Gerar PDF", width=18, command=gerar).pack(side="left", padx=8)
        tk.Button(btns, text="Cancelar", width=18, command=win.destroy).pack(side="left", padx=8)

    def _distinct_filter_values(self, db_path: str, col: str):
        vals = set()
        try:
            with connect(db_path) as con:
                cur = con.cursor()
                for table in ("fisico", "contabil"):
                    cur.execute(
                        f"SELECT DISTINCT TRIM(CAST({col} AS TEXT)) "
                        f"FROM {table} WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS TEXT)) <> ''"
                    )
                    for r in cur.fetchall():
                        if r and r[0]:
                            vals.add(str(r[0]))
        except Exception:
            pass
        return ["(TODOS)"] + sorted(vals)

    def _sanitize_desc(self, txt: str, limit: int = 50) -> str:
        # Mantém apenas caracteres alfabéticos e espaços para a descrição analítica.
        s = (txt or "").strip()
        s = "".join(ch for ch in s if ch.isalpha() or ch.isspace())
        s = " ".join(s.split())
        return s[:limit]

    def _coerce_filter_code(self, value: str):
        v = (value or "").strip()
        if not v or v == "(TODOS)":
            return None
        if v.isdigit():
            try:
                return int(v)
            except Exception:
                return v
        return v

    def _pick_col_name(self, con: sqlite3.Connection, table: str, preferred: str, fallback: str) -> str:
        cur = con.cursor()
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        if preferred in cols:
            return preferred
        if fallback in cols:
            return fallback
        low = {c.lower(): c for c in cols}
        return low.get(preferred.lower(), fallback)

    def _build_analitico_pairs_query(self, con: sqlite3.Connection, filial: str, ccusto: str, local: str):
        par_col = self._pick_col_name(con, "depara", "PAR_ID", "par_id")
        idf_col = self._pick_col_name(con, "depara", "ID_FISICO", "id_fisico")
        idc_col = self._pick_col_name(con, "depara", "ID_CONTABIL", "id_contabil")

        where = ["1=1"]
        params = []
        filial_v = self._coerce_filter_code(filial)
        ccusto_v = self._coerce_filter_code(ccusto)
        local_v = self._coerce_filter_code(local)
        if filial_v is not None:
            where.append("COALESCE(f.FILIAL, t.FILIAL) = ?")
            params.append(filial_v)
        if ccusto_v is not None:
            where.append("COALESCE(f.CCUSTO, t.CCUSTO) = ?")
            params.append(ccusto_v)
        if local_v is not None:
            where.append("COALESCE(f.LOCAL, t.LOCAL) = ?")
            params.append(local_v)

        sql = f"""
            SELECT
                d.{par_col},
                COALESCE(CAST(f.FILIAL AS TEXT), ''),
                COALESCE(CAST(f.CCUSTO AS TEXT), ''),
                COALESCE(CAST(f.LOCAL AS TEXT), ''),
                COALESCE(CAST(f.NRBRM AS TEXT), ''),
                COALESCE(CAST(f.INC AS TEXT), ''),
                COALESCE(CAST(f.DESCRICAO AS TEXT), ''),
                COALESCE(CAST(t.FILIAL AS TEXT), ''),
                COALESCE(CAST(t.CCUSTO AS TEXT), ''),
                COALESCE(CAST(t.LOCAL AS TEXT), ''),
                COALESCE(CAST(t.NRBRM AS TEXT), ''),
                COALESCE(CAST(t.INC AS TEXT), ''),
                COALESCE(CAST(t.DESCRICAO AS TEXT), '')
            FROM depara d
            LEFT JOIN fisico f ON f.ID = d.{idf_col}
            LEFT JOIN contabil t ON t.ID = d.{idc_col}
            WHERE {" AND ".join(where)}
            ORDER BY d.{par_col}
        """
        return sql, tuple(params)

    def _gerar_relatorio_analitico_pdf(self, db_path: str, output_path: str, *, filial: str = "", ccusto: str = "", local: str = "") -> None:
        try:
            from reportlab.lib.pagesizes import A4, landscape
            from reportlab.lib.units import mm
            from reportlab.pdfgen import canvas
        except Exception:
            raise RuntimeError(
                "Biblioteca 'reportlab' não está instalada.\n"
                "Instale com: python -m pip install reportlab"
            )

        c = canvas.Canvas(output_path, pagesize=landscape(A4))
        page_w, page_h = landscape(A4)

        left = 12 * mm
        right = page_w - 12 * mm
        y = page_h - 14 * mm
        row_h = 6.0 * mm
        fs_title = 13
        fs_head = 9
        fs_body = 8

        # colunas: BASE | FILIAL | CCUSTO | LOCAL | NRBEM | INC | DESCRIÇÃO
        widths = [22 * mm, 24 * mm, 28 * mm, 24 * mm, 28 * mm, 16 * mm]
        x = [left]
        for w in widths:
            x.append(x[-1] + w)
        x_desc = x[-1]
        filtro_txt = (
            f"FILTRO:  FILIAL={filial or '(TODOS)'}   "
            f"CCUSTO={ccusto or '(TODOS)'}   "
            f"LOCAL={local or '(TODOS)'}"
        )

        def header():
            nonlocal y
            y = page_h - 14 * mm
            c.setFont("Helvetica-Bold", fs_title)
            c.drawString(left, y, "Relatório Analítico da Conciliação")
            y -= 8 * mm
            c.setFont("Helvetica", fs_head)
            c.drawString(left, y, filtro_txt)
            y -= 7 * mm
            c.setFont("Helvetica-Bold", fs_head)
            c.drawString(x[0], y, "BASE")
            c.drawString(x[1], y, "FILIAL")
            c.drawString(x[2], y, "CCUSTO")
            c.drawString(x[3], y, "LOCAL")
            c.drawString(x[4], y, "NRBEM")
            c.drawString(x[5], y, "INC")
            c.drawString(x_desc, y, "DESCRIÇÃO")
            y -= 2 * mm
            c.line(left, y, right, y)
            y -= 4 * mm

        def ensure_space(lines: int = 1, extra_mm: float = 0.0):
            nonlocal y
            need = (lines * row_h) + (extra_mm * mm)
            if y < (15 * mm + need):
                c.showPage()
                header()

        def draw_base_row(base: str, fil, ccu, loc, nrbem, inc, desc_txt: str):
            nonlocal y
            c.drawString(x[0], y, base)
            c.drawString(x[1], y, str(fil)[:8])
            c.drawString(x[2], y, str(ccu)[:12])
            c.drawString(x[3], y, str(loc)[:10])
            c.drawString(x[4], y, str(nrbem)[:12])
            c.drawString(x[5], y, str(inc)[:6])
            c.drawString(x_desc, y, desc_txt[:50])
            y -= row_h

        def draw_rows_streamed():
            nonlocal y
            with connect(db_path) as con:
                sql, params = self._build_analitico_pairs_query(con, filial, ccusto, local)
                cur = con.cursor()
                cur.execute(sql, params)

                has_data = False
                c.setFont("Helvetica", fs_body)
                c.setStrokeColorRGB(0.75, 0.75, 0.75)

                while True:
                    chunk = cur.fetchmany(1200)
                    if not chunk:
                        break
                    for r in chunk:
                        has_data = True
                        ensure_space(lines=2, extra_mm=1.5)
                        draw_base_row(
                            "FIS",
                            r[1], r[2], r[3], r[4], r[5],
                            self._sanitize_desc(r[6], 50),
                        )
                        draw_base_row(
                            "CTB",
                            r[7], r[8], r[9], r[10], r[11],
                            self._sanitize_desc(r[12], 50),
                        )
                        c.line(left, y + 1.5 * mm, right, y + 1.5 * mm)
                        y -= 1.5 * mm

                if not has_data:
                    ensure_space(lines=1)
                    c.setStrokeColorRGB(0.0, 0.0, 0.0)
                    c.setFont("Helvetica", fs_body)
                    c.drawString(x[0], y, "-")
                    c.drawString(x[1], y, "(sem registros para o filtro)")
                    y -= row_h
                    return

                c.setStrokeColorRGB(0.0, 0.0, 0.0)

        header()
        draw_rows_streamed()

        c.setFont("Helvetica-Oblique", 8)
        c.drawRightString(
            right,
            8 * mm,
            f"Emitido em {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        )
        c.save()

    # ---------- métricas / mini-dashboard ----------
    def _detect_backend_label(self) -> str:
        db_path = self.var_db.get().strip()
        if not db_path:
            return "Banco ativo: não configurado"
        try:
            with connect(db_path) as con:
                con.execute("SELECT 1;")
                backend = str(getattr(con, "_evs_backend", "sqlite")).strip().lower()
            if backend == "postgres":
                return "Banco ativo: PostgreSQL"
            return "Banco ativo: SQLite"
        except Exception:
            return "Banco ativo: indisponível"

    def _can_connect_db(self, db_path: str) -> bool:
        try:
            with connect(db_path) as con:
                con.execute("SELECT 1;")
            return True
        except Exception:
            return False

    def _get_metrics(self) -> dict:
        db = self.var_db.get().strip()
        if not db:
            return {
                "fis_total": 0,
                "ctb_total": 0,
                "pares": 0,
                "fis_conc": 0,
                "ctb_conc": 0,
                "sobras_fis": 0,
                "sobras_ctb": 0,
            }

        con = connect(db)
        try:
            cur = con.cursor()

            def q1(sql: str, params=()):
                cur.execute(sql, params)
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else 0

            fis_total = q1("SELECT COUNT(*) FROM fisico")
            ctb_total = q1("SELECT COUNT(*) FROM contabil")
            pares = q1("SELECT COUNT(*) FROM depara")
            fis_conc = q1("SELECT COUNT(*) FROM conciliados WHERE BASE='FIS'")
            ctb_conc = q1("SELECT COUNT(*) FROM conciliados WHERE BASE='CTB'")
            return {
                "fis_total": fis_total,
                "ctb_total": ctb_total,
                "pares": pares,
                "fis_conc": fis_conc,
                "ctb_conc": ctb_conc,
                "sobras_fis": max(0, fis_total - fis_conc),
                "sobras_ctb": max(0, ctb_total - ctb_conc),
            }
        except Exception:
            return {
                "fis_total": 0,
                "ctb_total": 0,
                "pares": 0,
                "fis_conc": 0,
                "ctb_conc": 0,
                "sobras_fis": 0,
                "sobras_ctb": 0,
            }
        finally:
            con.close()

    def _update_dashboard(self) -> None:
        m = self._get_metrics()
        self.mv_backend.set(self._detect_backend_label())

        def fmt(n: int) -> str:
            try:
                return f"{int(n):,}".replace(",", ".")
            except Exception:
                return str(n)

        self.mv_fis_total.set(fmt(m["fis_total"]))
        self.mv_ctb_total.set(fmt(m["ctb_total"]))
        self.mv_pares.set(fmt(m["pares"]))
        self.mv_fis_conc.set(fmt(m["fis_conc"]))
        self.mv_ctb_conc.set(fmt(m["ctb_conc"]))
        self.mv_sobras_fis.set(fmt(m["sobras_fis"]))
        self.mv_sobras_ctb.set(fmt(m["sobras_ctb"]))

        # Resumo (1 linha) — Importados | Conciliados | Sobras
        self.mv_resumo.set(
            f"Importados — Físico: {self.mv_fis_total.get()} | Contábil: {self.mv_ctb_total.get()}    "
            f"Conciliados — Físico: {self.mv_fis_conc.get()} | Contábil: {self.mv_ctb_conc.get()}    "
            f"Sobras — Físico: {self.mv_sobras_fis.get()} | Contábil: {self.mv_sobras_ctb.get()}"
        )
        self.mv_line_import.set(
            f"Importados — Físico: {self.mv_fis_total.get()} | Contábil: {self.mv_ctb_total.get()}"
        )
        self.mv_line_conc.set(
            f"Conciliados — Físico: {self.mv_fis_conc.get()} | Contábil: {self.mv_ctb_conc.get()}"
        )
        self.mv_line_sobra.set(
            f"Sobras — Físico: {self.mv_sobras_fis.get()} | Contábil: {self.mv_sobras_ctb.get()}"
        )

    def _pick_fis(self):
        p = filedialog.askopenfilename(
            title="Selecione BsFisico.xlsx",
            filetypes=[("Excel", ("*.xlsx", "*.xlsm")), ("Todos", "*.*")],
            parent=self
        )
        if p:
            self.var_fis.set(p)

    def _pick_ctb(self):
        p = filedialog.askopenfilename(
            title="Selecione BsContabil.xlsx",
            filetypes=[("Excel", ("*.xlsx", "*.xlsm")), ("Todos", "*.*")],
            parent=self
        )
        if p:
            self.var_ctb.set(p)

    def _pick_tpl(self):
        p = filedialog.askopenfilename(
            title="Selecione Template BsDePara.xlsx",
            filetypes=[("Excel", ("*.xlsx", "*.xlsm")), ("Todos", "*.*")],
            parent=self
        )
        if p:
            self.var_tpl.set(p)

    def _pick_out(self):
        p = filedialog.asksaveasfilename(
            title="Salvar BsDePara (saída)",
            defaultextension=".xlsx",
            filetypes=[("Excel", ("*.xlsx",)), ("Todos", "*.*")],
            parent=self
        )
        if p:
            self.var_out.set(p)

    def _pick_db(self):
        p = filedialog.askopenfilename(
            title="Selecione o arquivo do banco (.db)",
            filetypes=[("SQLite", ("*.db",)), ("Todos", "*.*")],
            parent=self
        )
        if p:
            self.var_db.set(p)
            self._update_dashboard()

    # ---------- actions ----------
    def _cleanup_cache_on_exit(self) -> tuple[int, int]:
        """Limpa caches/artefatos de runtime para reduzir acúmulo e lentidão."""
        removed_dirs = 0
        removed_files = 0
        base = os.path.abspath(getattr(self, "base_dir", os.path.dirname(__file__)))
        skip_top = {"dist", "build", ".git", ".idea", ".vscode", ".venv", "venv"}

        # 1) Cache Python local (somente código-fonte; ignora artefatos de build).
        for root, dirs, files in os.walk(base, topdown=True):
            dirs[:] = [d for d in dirs if d not in skip_top]

            if "__pycache__" in dirs:
                p = os.path.join(root, "__pycache__")
                try:
                    shutil.rmtree(p, ignore_errors=False)
                    removed_dirs += 1
                except Exception:
                    pass
                dirs.remove("__pycache__")

            for f in files:
                if not (f.endswith(".pyc") or f.endswith(".pyo")):
                    continue
                p = os.path.join(root, f)
                try:
                    os.remove(p)
                    removed_files += 1
                except Exception:
                    pass

        # 2) Arquivos transitórios do SQLite (WAL/SHM/JOURNAL) dos DBs usados.
        db_candidates = set()
        db_var = self.var_db.get().strip() if hasattr(self, "var_db") else ""
        if db_var:
            db_candidates.add(db_var)
        db_candidates.add(os.path.join(base, "conciliador.db"))
        db_candidates.add(os.path.join(base, "conciliador_v2.db"))

        for db_path in db_candidates:
            if not db_path:
                continue
            db_path = os.path.abspath(db_path)
            if not os.path.exists(db_path):
                continue

            try:
                con = connect(db_path)
                try:
                    con.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                finally:
                    con.close()
            except Exception:
                pass

            for suffix in ("-wal", "-shm", "-journal"):
                side = f"{db_path}{suffix}"
                if not os.path.exists(side):
                    continue
                try:
                    os.remove(side)
                    removed_files += 1
                except Exception:
                    pass

        # 3) Lixo temporário típico do Excel na pasta temporária do sistema.
        try:
            tmp_dir = tempfile.gettempdir()
            for name in os.listdir(tmp_dir):
                if not name.startswith("~$"):
                    continue
                p = os.path.join(tmp_dir, name)
                if not os.path.isfile(p):
                    continue
                try:
                    os.remove(p)
                    removed_files += 1
                except Exception:
                    pass
        except Exception:
            pass
        return removed_dirs, removed_files

    def _exit_system(self) -> None:
        try:
            d, f = self._cleanup_cache_on_exit()
            self._set_status(f"Saindo... cache limpo ({d} pasta(s), {f} arquivo(s)).")
        except Exception:
            pass
        self.destroy()

    def _set_status(self, msg: str) -> None:
        self.lbl_status.config(text=msg)
        self.update_idletasks()


    def _set_buttons_enabled(self, enabled: bool) -> None:
        # desabilita botões durante tarefas pesadas para evitar duplo clique e travamento
        for btn in getattr(self, "_action_buttons", []):
            try:
                btn.config(state=("normal" if enabled else "disabled"))
            except Exception:
                pass

    def _run_async(self, start_msg: str, work_fn, ok_title: str, ok_msg: str, end_status: str) -> None:
        """Executa tarefas pesadas fora da thread do Tkinter para não congelar a UI."""
        self._set_status(start_msg)
        self._set_buttons_enabled(False)

        def runner():
            try:
                result = work_fn()
                def on_ok():
                    if ok_msg:
                        messagebox.showinfo(ok_title, ok_msg)
                    self._set_status(end_status)
                    self._update_dashboard()
                    self._set_buttons_enabled(True)
                self.after(0, on_ok)
                return result
            except Exception as e:
                def on_err():
                    messagebox.showerror("Erro", str(e))
                    self._set_status(f"Erro: {e}")
                    self._set_buttons_enabled(True)
                self.after(0, on_err)

        threading.Thread(target=runner, daemon=True).start()

    def _import_bases(self):
        fis = self.var_fis.get().strip()
        ctb = self.var_ctb.get().strip()
        dbp = self.var_db.get().strip()

        if not os.path.isfile(fis):
            messagebox.showerror("Erro", "Arquivo BsFisico não encontrado.")
            return
        if not os.path.isfile(ctb):
            messagebox.showerror("Erro", "Arquivo BsContabil não encontrado.")
            return
        if not dbp:
            messagebox.showerror("Erro", "Informe o caminho do banco (.db).")
            return

        self._run_async(
            start_msg="Importando bases…",
            work_fn=lambda: import_bases(fis, ctb, dbp, reset=True),
            ok_title="OK",
            ok_msg="Importação concluída.",
            end_status="Importação concluída."
        )

    def _processar_auto(self):
        dbp = self.var_db.get().strip()
        if not dbp:
            messagebox.showerror("Erro", "Selecione o banco (.db).")
            return
        self._run_async(
            start_msg="Processando automático…",
            work_fn=lambda: run_auto_main(dbp),
            ok_title="OK",
            ok_msg="Processamento automático finalizado.",
            end_status="Automático finalizado."
        )

    def _exportar_bsdepara(self):
        dbp = self.var_db.get().strip()
        tpl = self.var_tpl.get().strip()
        outp = self.var_out.get().strip()

        if not dbp:
            messagebox.showerror("Erro", "Selecione o banco (.db).")
            return
        if not os.path.isfile(tpl):
            messagebox.showerror("Erro", "Selecione um template BsDePara.xlsx válido.")
            return
        if not outp:
            messagebox.showerror("Erro", "Informe o arquivo de saída.")
            return

        # cria pasta se necessário
        os.makedirs(os.path.dirname(outp) or ".", exist_ok=True)

        self._run_async(
            start_msg="Exportando BsDePara…",
            work_fn=lambda: export_bsdepara(dbp, tpl, outp, ultra_fast=False),
            ok_title="OK",
            ok_msg=f"Exportação concluída:\n{outp}",
            end_status="Exportação concluída."
        )

    def _abrir_manual(self):
        dbp = self.var_db.get().strip()
        if not dbp:
            messagebox.showerror("Erro", "Selecione o banco (.db).")
            return
        ManualV2Window(self, db_path=dbp)

    def _abrir_dashboard(self):
        dbp = self.var_db.get().strip()
        if not dbp:
            messagebox.showerror("Erro", "Selecione o banco (.db).")
            return
        DashboardWindow(self, db_path=dbp)

    def _abrir_dashboard_sintetico(self):
        dbp = self.var_db.get().strip()
        if not dbp:
            messagebox.showerror("Erro", "Selecione o banco (.db).")
            return
        try:
            # Importa em runtime para evitar problemas de import circular
            from dashboard_v2 import DashboardSintetico
            DashboardSintetico(self, db_path=dbp)
        except Exception as e:
            messagebox.showerror("Dashboard Sintético", f"Não foi possível abrir o dashboard sintético.\n\nErro: {e}")


    def _importar_depara(self):
        dbp = self.var_db.get().strip()
        if not dbp:
            messagebox.showerror("Erro", "Selecione o banco (.db).")
            return
        DeParaImportWindow(self, db_path=dbp)

if __name__ == "__main__":
    app = TelaInicialV2()
    app.mainloop()
# Layout fix: Modo Conciliação moved beside Modo.
