from __future__ import annotations

import re
import tkinter as tk
from tkinter import ttk, messagebox
from tkinter import font as tkfont

import pandas as pd
from datetime import datetime

from manual_db_v2_fixed import (
    connect,
    get_counts,
    load_pending_manual,
    load_pairs_auto02,
    get_distinct_values,
    save_manual_pairs,
    AUTO02_RULES,
    find_children_ctb_ids,
)

BG = "#225781"
MANUAL_MAX_ROWS = 2000  # limite de linhas por lado (Físico/Contábil) na tela Manual


class ValuePicker(tk.Toplevel):
    def __init__(self, master, title: str, values: list[str], target_var: tk.StringVar):
        super().__init__(master)
        self.title(title)
        self.configure(bg=BG)
        self.resizable(False, False)
        self.target_var = target_var
        self.values_all = values[:] if values else []
        self.values_filtered = self.values_all[:]

        frm = tk.Frame(self, bg=BG)
        frm.pack(padx=10, pady=10, fill="both", expand=True)

        tk.Label(frm, text="Pesquisar:", fg="white", bg=BG).grid(row=0, column=0, sticky="w")
        self.var_search = tk.StringVar()
        ent = tk.Entry(frm, textvariable=self.var_search, width=40)
        ent.grid(row=0, column=1, sticky="we", padx=(6,0))
        ent.bind("<KeyRelease>", lambda e: self._apply_filter())
        ent.focus_set()

        self.listbox = tk.Listbox(frm, height=14, width=45)
        self.listbox.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8,0))
        self.listbox.bind("<Double-Button-1>", lambda e: self._confirm())

        sb = tk.Scrollbar(frm, orient="vertical", command=self.listbox.yview)
        sb.grid(row=1, column=2, sticky="ns", pady=(8,0))
        self.listbox.configure(yscrollcommand=sb.set)

        btns = tk.Frame(frm, bg=BG)
        btns.grid(row=2, column=0, columnspan=3, sticky="e", pady=(10,0))
        tk.Button(btns, text="OK", width=10, command=self._confirm).pack(side="right", padx=(6,0))
        tk.Button(btns, text="Cancelar", width=10, command=self.destroy).pack(side="right")

        frm.grid_columnconfigure(1, weight=1)

        self._refresh()

        self.update_idletasks()
        x = master.winfo_rootx() + (master.winfo_width() // 2) - (self.winfo_width() // 2)
        y = master.winfo_rooty() + (master.winfo_height() // 2) - (self.winfo_height() // 2)
        self.geometry(f"+{max(0,x)}+{max(0,y)}")

        self.transient(master)
        self.grab_set()

    def _apply_filter(self):
        q = (self.var_search.get() or "").strip().lower()
        if not q:
            self.values_filtered = self.values_all[:]
        else:
            self.values_filtered = [v for v in self.values_all if q in (v or "").lower()]
        self._refresh()

    def _refresh(self):
        self.listbox.delete(0, tk.END)
        for v in self.values_filtered:
            self.listbox.insert(tk.END, v)

    def _confirm(self):
        sel = self.listbox.curselection()
        if not sel:
            return
        val = self.listbox.get(sel[0])
        self.target_var.set(val)
        self.destroy()


def _clean_cell(x) -> str:
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    if x is None:
        return ""
    return str(x).replace("\n", " ").replace("\r", " ")


class ManualV2Window(tk.Toplevel):
    """
    manual_v2_FINAL.py

    FINAL (seleção B = clique simples):
    - 2 grids (FIS / CTB) com scroll vertical + horizontal (sempre visíveis)
    - Sem checkbox congelado
    - Seleção livre: 1 linha FIS + 1 linha CTB => cria par
    - Painel "Pares pendentes" com remover/limpar
    - CTB na grade: somente INC=0 (pais)
    - Ao conciliar pai: pergunta se concilia incorporados (INC≠0) e adiciona aos pendentes (replicando físico)
    - Pares pendentes exibem (PAI) / (FILHO) (visual apenas)
    - Permite criar vários pares e salvar de uma vez
    """

    def __init__(self, master: tk.Misc, db_path: str):
        super().__init__(master)
        self.db_path = db_path
        self.title("Conciliação Manual – Sobras do Automático")
        self.state("zoomed")
        self.configure(bg=BG)

        # filtros
        self.var_desc1 = tk.StringVar(value="")
        self.var_desc2 = tk.StringVar(value="")
        self.var_desc3 = tk.StringVar(value="")
        self.var_desc_mode = tk.StringVar(value="E")

        self.var_filial = tk.StringVar(value="")
        self.var_ccusto = tk.StringVar(value="")
        self.var_local = tk.StringVar(value="")
        self.var_condic = tk.StringVar()
        self.var_data_ctb = tk.StringVar(value="")

        # modo
        self.var_mode = tk.StringVar(value="Manual")
        self.var_auto02_rule = tk.StringVar(value="")

        # contadores
        self.var_pf = tk.StringVar(value="0")
        self.var_pc = tk.StringVar(value="0")
        self.var_pp = tk.StringVar(value="0")

        # dados
        self.df_fis = pd.DataFrame()
        self.df_ctb = pd.DataFrame()

        # pendentes com metadados (role = "PAI" | "FILHO")
        # item: {"fis_id":int,"ctb_id":int,"role":str,"fis_row":int,"ctb_row":int}
        self.pending_pairs: list[dict] = []

        # controle de lotes (para desfazer o último clique de pareamento)
        self._batch_seq: int = 0

        # contadores (para remoção/cores corretas quando houver pai + filhos)
        self._fis_id_count: dict[int, int] = {}
        self._ctb_id_count: dict[int, int] = {}
        self._fis_row_count: dict[int, int] = {}
        self._ctb_row_count: dict[int, int] = {}

        self._build()
        self._refresh_counts_only()
        self._clear_tables()

    # ---------------- UI ----------------
    def _build(self):
        top = tk.Frame(self, bg=BG)
        top.pack(fill="x", padx=16, pady=10)

        tk.Label(
            top,
            text="FILTRO – CONCILIAÇÃO MANUAL",
            fg="white",
            bg=BG,
            font=("Arial", 14, "bold"),
        ).pack(anchor="w")

        # Linha 1: Descrições + modo
        row1 = tk.Frame(top, bg=BG)
        row1.pack(fill="x", pady=(10, 4))

        self._add_entry(row1, "Descrição_01:", self.var_desc1, width=32)
        self._add_entry(row1, "Descrição_02:", self.var_desc2, width=32, padx=(18, 6))
        self._add_entry(row1, "Descrição_03:", self.var_desc3, width=32, padx=(18, 6))

        tk.Label(row1, text="Modo:", fg="white", bg=BG).pack(side="left", padx=(18, 6))
        ttk.Combobox(
            row1,
            textvariable=self.var_desc_mode,
            values=["E", "OU"],
            width=6,
            state="readonly",
        ).pack(side="left")


        # Modo de conciliação ao lado do "Modo" (para não sair da visualização)
        tk.Label(row1, text="Modo Conciliação:", fg="white", bg=BG).pack(side="left", padx=(18, 6))
        self.cmb_mode = ttk.Combobox(
            row1,
            textvariable=self.var_mode,
            values=["Manual", "Automático (02)"],
            width=18,
            state="readonly",
        )
        self.cmb_mode.pack(side="left")
        self.cmb_mode.bind("<<ComboboxSelected>>", lambda e: self._on_mode_change())

        # Linha 2: Filial/CC/Local/Data + modo + tipo Auto02
        row2 = tk.Frame(top, bg=BG)
        row2.pack(fill="x", pady=(8, 0))

        self._add_entry(row2, "Filial:", self.var_filial, width=14, picker_field="FILIAL")
        self._add_entry(row2, "Centro de Custo:", self.var_ccusto, width=18, padx=(18, 6), picker_field="CCUSTO")
        self._add_entry(row2, "Local:", self.var_local, width=14, padx=(18, 6), picker_field="LOCAL")
        self._add_entry(row2, "Condição de Uso:", self.var_condic, width=14, padx=(18, 6), picker_field="CONDIC")
        self._add_entry(row2, "Ano CTB:", self.var_data_ctb, width=8, padx=(18, 6), only_year=True, picker_field="ANO_CTB")
        tk.Label(row2, text="Tipo Auto02:", fg="white", bg=BG).pack(side="left", padx=(18, 6))
        self.cmb_rule = ttk.Combobox(
            row2,
            textvariable=self.var_auto02_rule,
            values=[""] + [f"{k} - {v}" for k, v in AUTO02_RULES.items()],
            width=38,
            state="readonly",
        )
        self.cmb_rule.pack(side="left")

        # Linha 3: botões + contadores
        row3 = tk.Frame(top, bg=BG)
        row3.pack(fill="x", pady=(12, 0))

        tk.Button(row3, text="Aplicar Filtros", width=14, command=self._apply_filters).pack(side="left", padx=(0, 10))
        tk.Button(row3, text="Limpar", width=10, command=self._clear_filters).pack(side="left")

        cnt = tk.Frame(row3, bg=BG)
        cnt.pack(side="right")

        tk.Label(cnt, text="Físico pendente:", fg="white", bg=BG).pack(side="left", padx=10)
        tk.Label(cnt, textvariable=self.var_pf, fg="white", bg=BG, font=("Arial", 11, "bold")).pack(side="left")

        tk.Label(cnt, text="Contábil pendente:", fg="white", bg=BG).pack(side="left", padx=10)
        tk.Label(cnt, textvariable=self.var_pc, fg="white", bg=BG, font=("Arial", 11, "bold")).pack(side="left")

        tk.Label(cnt, text="Pares (staging):", fg="white", bg=BG).pack(side="left", padx=10)
        tk.Label(cnt, textvariable=self.var_pp, fg="white", bg=BG, font=("Arial", 11, "bold")).pack(side="left")


        # LOG (mensagens rápidas)
        logf = tk.Frame(top, bg=BG)
        logf.pack(fill="x", pady=(10, 0))
        tk.Label(logf, text="STATUS:", fg="white", bg=BG).pack(side="left", padx=(0, 8))

        self.status_var = tk.StringVar(value="Pronto.")
        # Verde suave (sucesso) por padrão
        self.lbl_status = tk.Label(
            logf,
            textvariable=self.status_var,
            fg="white",
            bg="#1e5a3a",
            anchor="w",
            justify="left",
            padx=10,
            pady=6,
        )
        self.lbl_status.pack(side="left", fill="x", expand=True)

# ---------- centro: grids + painel pendentes ----------
        mid = tk.Frame(self, bg=BG)
        mid.pack(fill="both", expand=True, padx=16, pady=10)

        mid.grid_rowconfigure(0, weight=3)
        mid.grid_rowconfigure(1, weight=1)
        mid.grid_columnconfigure(0, weight=1, uniform="tables")
        mid.grid_columnconfigure(1, weight=0)
        mid.grid_columnconfigure(2, weight=1, uniform="tables")

        left = tk.Frame(mid, bg=BG)
        right = tk.Frame(mid, bg=BG)
        sep = ttk.Separator(mid, orient="vertical")

        left.grid(row=0, column=0, sticky="nsew")
        sep.grid(row=0, column=1, sticky="ns", padx=10)
        right.grid(row=0, column=2, sticky="nsew")

        tk.Label(left, text="FÍSICO (FRAG vazio)", fg="white", bg=BG, font=("Arial", 12, "bold")).pack(anchor="w")
        tk.Label(right, text="CONTÁBIL (FRAG vazio | INC=0)", fg="white", bg=BG, font=("Arial", 12, "bold")).pack(anchor="w")

        self.tv_fis = self._make_tree(left, kind="FIS")
        self.tv_ctb = self._make_tree(right, kind="CTB")


        # seleção: criar par automaticamente quando houver 1 linha selecionada em cada grade
        self._sel_f_row = None
        self._sel_c_row = None
        self._suspend_autopair = False
        self.tv_fis.bind("<<TreeviewSelect>>", lambda e: self._on_tree_select("FIS"))
        self.tv_ctb.bind("<<TreeviewSelect>>", lambda e: self._on_tree_select("CTB"))


        # Painel pendentes (embaixo, largura total)
        pending_frame = tk.Frame(mid, bg=BG)
        pending_frame.grid(row=1, column=0, columnspan=3, sticky="nsew", pady=(10, 0))
        pending_frame.grid_columnconfigure(0, weight=1)
        pending_frame.grid_rowconfigure(1, weight=1)

        tk.Label(
            pending_frame,
            text="PARES PENDENTES (antes de salvar)",
            fg="white",
            bg=BG,
            font=("Arial", 11, "bold"),
        ).grid(row=0, column=0, sticky="w")

        list_wrap = tk.Frame(pending_frame, bg=BG)
        list_wrap.grid(row=1, column=0, sticky="nsew")
        list_wrap.grid_rowconfigure(0, weight=1)
        list_wrap.grid_columnconfigure(0, weight=1)

        self.lb_pending = tk.Listbox(list_wrap, height=6)
        self.lb_pending.grid(row=0, column=0, sticky="nsew")

        sb_lb = tk.Scrollbar(
            list_wrap,
            orient="vertical",
            command=self.lb_pending.yview,
            width=16,
            bd=1,
            relief="sunken",
        )
        sb_lb.grid(row=0, column=1, sticky="ns")
        self.lb_pending.configure(yscrollcommand=sb_lb.set)

        actions = tk.Frame(pending_frame, bg=BG)
        actions.grid(row=1, column=1, sticky="ns", padx=(10, 0))
        tk.Button(actions, text="Remover Selecionados", width=18, command=self._remove_selected_pending).pack(pady=(0, 8))
        tk.Button(actions, text="Desfazer Último", width=18, command=self._undo_last_pending).pack(pady=(0, 8))
        tk.Button(actions, text="Conciliar", width=18, command=self._save_pairs).pack(pady=(0, 8))
        tk.Button(actions, text="Voltar", width=18, command=self.destroy).pack()

        # tags de cor para pendentes
        self.tv_fis.tag_configure("PENDING", background="#CFE8FF", foreground="black")
        self.tv_ctb.tag_configure("PENDING", background="#CFE8FF", foreground="black")

        self._on_mode_change()

    def _add_entry(self, parent, label, var, width=18, padx=(0, 6), only_year: bool = False, picker_field: str | None = None):
        tk.Label(parent, text=label, fg="white", bg=BG).pack(side="left", padx=padx)

        vcmd = None
        if only_year:
            vcmd = (self.register(lambda P: (P.isdigit() and len(P) <= 4) or P == ""), "%P")

        ent = tk.Entry(parent, textvariable=var, width=width, validate="key" if vcmd else "none", validatecommand=vcmd)
        ent.pack(side="left")

        if picker_field:
            tk.Button(
                parent,
                text="▾",
                width=2,
                command=lambda: self._open_picker(picker_field, var),
            ).pack(side="left", padx=(4, 10))

    def _open_picker(self, field: str, target_var: tk.StringVar):
        try:
            with connect(self.db_path) as _con:
                values = get_distinct_values(_con, field)
            if not values:
                messagebox.showinfo("Lista vazia", f"Não há valores disponíveis para {field}.")
                return
            ValuePicker(self, f"Selecionar {field}", values, target_var)
        except Exception as e:
            messagebox.showerror("Erro", f"Não foi possível carregar a lista de {field}.\n\n{e}")


    def _make_tree(self, parent: tk.Frame, *, kind: str) -> ttk.Treeview:
        outer = tk.Frame(parent, bg=BG)
        outer.pack(fill="both", expand=True, pady=(6, 0))

        if kind == "FIS":
            cols = ("ID", "Filial", "CCusto", "Local", "Condic", "NrBrm", "Inc.", "Descricao", "Marca", "Modelo", "Serie", "Dimensao", "Capacidade", "Tag")
        else:
            cols = ("ID", "Filial", "CCusto", "Local", "NrBrm", "Inc.", "Descricao","Serie","Tag","Data", "Vlr. Aquisição", "Depr. Acum.", "Residual")

        tv = ttk.Treeview(outer, columns=cols, show="headings", selectmode="browse")
        tv.grid(row=0, column=0, sticky="nsew")

        outer.grid_rowconfigure(0, weight=1)
        outer.grid_columnconfigure(0, weight=1)

        for c in cols:
            tv.heading(c, text=c)

        # Scrollbars (sempre visíveis)
        sb_kwargs = dict(width=16, bd=1, relief="sunken")
        vs = tk.Scrollbar(outer, orient="vertical", command=tv.yview, **sb_kwargs)
        vs.grid(row=0, column=1, sticky="ns")
        hs = tk.Scrollbar(outer, orient="horizontal", command=tv.xview, **sb_kwargs)
        hs.grid(row=1, column=0, sticky="ew")

        tv.configure(yscrollcommand=vs.set, xscrollcommand=hs.set)

        # larguras iniciais (depois autoajusta)
        if kind == "FIS":
            widths = {
                "ID": 60,
                "Filial": 70,
                "CCusto": 80,
                "Local": 70,
                "Condic": 90,
                "NrBrm": 90,
                "Inc.": 60,
                "Descricao": 420,
                "Marca": 120,
                "Modelo": 120,
                "Serie": 120,
                "Dimensao": 110,
                "Capacidade": 110,
                "Tag": 90,
            }
            # Texto à esquerda; numéricos à direita
            anchors = {c: ("w" if c in ("Descricao", "Marca", "Modelo", "Serie", "Dimensao", "Capacidade", "Tag", "Condic") else "e") for c in cols}
            anchors["Filial"] = "e"
            anchors["CCusto"] = "e"
            anchors["Local"] = "e"
        else:
            widths = {
                "ID": 60,
                "Filial": 70,
                "CCusto": 80,
                "Local": 70,
                "NrBrm": 110,
                "Inc.": 60,
                "Descricao": 420,
                "Serie": 320,
                "Tag": 220,
                "Data": 90,
                "Vlr. Aquisição": 120,
                "Depr. Acum.": 120,
                "Residual": 110,
            }
            anchors = {c: ("w" if c in ("Descricao","Serie","Tag") else "e") for c in cols}
            anchors["Data"] = "center"

        for c in cols:
            tv.column(c, width=widths.get(c, 100), anchor=anchors.get(c, "w"), stretch=False)

        return tv

    # ---------- Column autosize (visual) ----------
    def _get_tree_font(self) -> tkfont.Font:
        try:
            style_font = ttk.Style().lookup("Treeview", "font")
            if style_font:
                return tkfont.nametofont(style_font) if isinstance(style_font, str) else tkfont.Font(font=style_font)
        except Exception:
            pass
        return tkfont.nametofont("TkDefaultFont")

    def _autosize_columns(self, tv: ttk.Treeview, sample_limit: int = 250):
        cols = tv["columns"]
        if not cols:
            return
        font = self._get_tree_font()
        padding = 18
        max_width = {
            "Descricao": 1600,
            "Marca": 320,
            "Modelo": 320,
            "Serie": 320,
            "Dimensao": 260,
            "Capacidade": 260,
            "Tag": 220,
            "Data": 140,
            "Vlr. Aquisição": 220,
            "Depr. Acum.": 220,
            "Residual": 220,
            "NrBrm": 180,
            "Inc.": 90,
            "ID": 90,
        }

        children = tv.get_children()
        if len(children) > sample_limit:
            children = children[:sample_limit]

        for c in cols:
            w = font.measure(str(c)) + padding
            limit = max_width.get(c, 400)
            for iid in children:
                try:
                    cell = tv.set(iid, c)
                except Exception:
                    continue
                s = _clean_cell(cell)
                if not s:
                    continue
                w = max(w, font.measure(s) + padding)
                if w >= limit:
                    w = limit
                    break
            tv.column(c, width=min(w, limit), stretch=False)

    # ---------- Mode behavior ----------
    def _on_mode_change(self):
        is_auto = self.var_mode.get() == "Automático (02)"
        self.cmb_rule.configure(state="readonly" if is_auto else "disabled")
        if not is_auto:
            self.var_auto02_rule.set("")
        self._clear_tables()
        self._clear_pending()
        self._refresh_counts_only()


    # ---------- LOG ----------
    def _log(self, msg: str, level: str = "ok"):
        """Mostra apenas a última ação (status)."""
        ts = datetime.now().strftime("%H:%M:%S")
        text = f"[{ts}] {msg}"

        if hasattr(self, "status_var") and self.status_var is not None:
            self.status_var.set(text)

        # cor por nível
        bg_ok = "#1e5a3a"      # verde suave
        bg_warn = "#8a6d1f"    # amarelo/mostarda suave
        bg_err = "#7a2a2a"     # vermelho suave

        bg = bg_ok
        if level == "warn" or "Nenhuma referência" in msg or msg.startswith("⚠"):
            bg = bg_warn
        if level == "err" or msg.startswith("❌"):
            bg = bg_err

        if hasattr(self, "lbl_status") and self.lbl_status is not None:
            try:
                self.lbl_status.configure(bg=bg)
            except Exception:
                pass

    def _on_tree_select(self, which: str):
        if getattr(self, "_suspend_autopair", False):
            return

        if which == "FIS":
            sel = self.tv_fis.selection()
            self._sel_f_row = int(sel[0]) if sel else None
        else:
            sel = self.tv_ctb.selection()
            self._sel_c_row = int(sel[0]) if sel else None

        # cria par quando houver 1 seleção em cada lado
        if self._sel_f_row is None or self._sel_c_row is None:
            return

        row_f = int(self._sel_f_row)
        row_c = int(self._sel_c_row)

        # evita erro se as tabelas ainda não estão populadas
        if self.df_fis is None or self.df_ctb is None or self.df_fis.empty or self.df_ctb.empty:
            return

        created = self._create_pair_rows(row_f, row_c, silent=True)
        # limpa seleção para permitir cliques em sequência sem recriar
        if created:
            self._suspend_autopair = True
            try:
                self.tv_fis.selection_remove(self.tv_fis.selection())
                self.tv_ctb.selection_remove(self.tv_ctb.selection())
            finally:
                self._suspend_autopair = False
                self._sel_f_row = None
                self._sel_c_row = None

    def _create_pair_rows(self, row_f: int, row_c: int, *, silent: bool = False) -> bool:
        """Cria o par usando índices (linhas) das grades.
        Retorna True se criou (ou adicionou filhos), False se não criou (duplicado/ inválido).
        """
        if self.df_fis.empty or self.df_ctb.empty:
            return False
        def _pk_from_row(r):
            # Preferir ID de negócio; usar row_id apenas como fallback de compatibilidade.
            for k in ("ID", "Id", "id"):
                v = r.get(k, None)
                if v is None:
                    continue
                s = str(v).strip()
                if not s or s.lower() == "nan":
                    continue
                try:
                    return int(float(s))
                except Exception:
                    pass
            for k in ("row_id", "ROW_ID", "rowid", "ROWID"):
                v = r.get(k, None)
                if v is None:
                    continue
                s = str(v).strip()
                if not s or s.lower() == "nan":
                    continue
                try:
                    return int(float(s))
                except Exception:
                    pass
            return 0

        id_f = _pk_from_row(self.df_fis.iloc[row_f])
        id_c = _pk_from_row(self.df_ctb.iloc[row_c])

        if not id_f or not id_c:
            if not silent:
                messagebox.showwarning("Atenção", "Seleção inválida.")
            self._log("⚠️ Seleção inválida (IDs vazios).")
            return False

        # bloqueios: físico e contábil não podem repetir enquanto pendentes
        if self._fis_id_count.get(id_f, 0) > 0:
            self._log(f"⚠️ Físico {id_f} já está em par pendente.")
            return False
        if self._ctb_id_count.get(id_c, 0) > 0:
            self._log(f"⚠️ Contábil {id_c} já está em par pendente.")
            return False

        # controle de lote para desfazer
        self._batch_seq += 1
        batch = self._batch_seq

        # adiciona par principal (PAI)
        self.pending_pairs.append(
            {"fis_id": id_f, "ctb_id": id_c, "role": "PAI", "fis_row": row_f, "ctb_row": row_c, "batch": batch}
        )

        # incrementa contadores
        self._fis_id_count[id_f] = self._fis_id_count.get(id_f, 0) + 1
        self._ctb_id_count[id_c] = self._ctb_id_count.get(id_c, 0) + 1
        self._fis_row_count[row_f] = self._fis_row_count.get(row_f, 0) + 1
        self._ctb_row_count[row_c] = self._ctb_row_count.get(row_c, 0) + 1

        # pinta linhas
        self.tv_fis.item(str(row_f), tags=("PENDING",))
        self.tv_ctb.item(str(row_c), tags=("PENDING",))

        # regra: se contábil é pai (INC=0), pergunta pelos filhos
        try:
            inc_val = int(float(self.df_ctb.iloc[row_c].get("INC", 0) or 0))
        except Exception:
            inc_val = 0
        try:
            nrbrm = int(float(self.df_ctb.iloc[row_c].get("NRBRM", 0) or 0))
        except Exception:
            nrbrm = 0

        extra_children = 0
        if nrbrm and inc_val == 0:
            try:
                with connect(self.db_path) as con:
                    child_ids = find_children_ctb_ids(con, nrbrm=nrbrm, exclude_ctb_id=id_c, limit=2000)
            except Exception:
                child_ids = []

            if child_ids:
                msg = (
                    f"Encontrados {len(child_ids)} item(ns) incorporado(s) (INC≠0) para o NrBrm {nrbrm}.\n\n"
                    "Deseja conciliar também?"
                )
                if messagebox.askyesno("Incorporações", msg):
                    for cid in child_ids:
                        cid = int(cid)
                        if self._ctb_id_count.get(cid, 0) > 0:
                            continue
                        self.pending_pairs.append(
                            {"fis_id": id_f, "ctb_id": cid, "role": "FILHO", "fis_row": row_f, "ctb_row": row_c, "batch": batch}
                        )
                        self._fis_id_count[id_f] = self._fis_id_count.get(id_f, 0) + 1
                        self._ctb_id_count[cid] = self._ctb_id_count.get(cid, 0) + 1
                        self._fis_row_count[row_f] = self._fis_row_count.get(row_f, 0) + 1
                        self._ctb_row_count[row_c] = self._ctb_row_count.get(row_c, 0) + 1
                        extra_children += 1

        self._rebuild_pending_listbox()
        self._log(f"➕ Par adicionado: FIS {id_f} ↔ CTB {id_c} (pendentes: {len(self.pending_pairs)})" + (f" +{extra_children} filho(s)" if extra_children else ""))
        return True
    # ---------- Filters ----------
    def _filters_payload(self) -> dict:
        return dict(
            desc1=self.var_desc1.get(),
            desc2=self.var_desc2.get(),
            desc3=self.var_desc3.get(),
            desc_mode=self.var_desc_mode.get(),
            filial=self.var_filial.get(),
            ccusto=self.var_ccusto.get(),
            local=self.var_local.get(),
            condic=self.var_condic.get(),
            data_ctb_ano=re.sub(r"\D", "", self.var_data_ctb.get())[:4],
        )

    def _clear_filters(self):
        self.var_desc1.set("")
        self.var_desc2.set("")
        self.var_desc3.set("")
        self.var_desc_mode.set("E")
        self.var_filial.set("")
        self.var_ccusto.set("")
        self.var_local.set("")
        self.var_condic.set("")
        self.var_data_ctb.set("")
        self._clear_tables()
        self._clear_pending()
        self._refresh_counts_only()

    def _apply_filters(self):
        payload = self._filters_payload()
        mode = self.var_mode.get()
        auto_pairs: list[tuple[int, int]] = []

        with connect(self.db_path) as con:
            if mode == "Manual":
                df_f = load_pending_manual(con, "FIS", limit=MANUAL_MAX_ROWS, **payload)
                df_c = load_pending_manual(con, "CTB", limit=MANUAL_MAX_ROWS, only_inc0=True, **payload)
            else:
                rid = (self.var_auto02_rule.get() or "").split(" - ")[0].strip()
                if not rid:
                    self._clear_tables()
                    self._clear_pending()
                    self._refresh_counts_only()
                    return
                df_f, df_c, _pairs = load_pairs_auto02(con, rid, limit_pairs=500, **payload)
                auto_pairs = [(int(a), int(b)) for a, b in _pairs]

                # Mesmo no Auto02, mantemos CTB INC=0 na grade (critério seu)
                if "INC" in df_c.columns:
                    try:
                        df_c = df_c[df_c["INC"].fillna(0).astype(float).astype(int) == 0].reset_index(drop=True)
                    except Exception:
                        pass

        self.df_fis = df_f.reset_index(drop=True)
        self.df_ctb = df_c.reset_index(drop=True)

        self._populate(self.tv_fis, self.df_fis, base="FIS")
        self._populate(self.tv_ctb, self.df_ctb, base="CTB")

        if mode == "Automático (02)":
            self._stage_auto02_pairs(auto_pairs)

        self._refresh_counts_only()

        # LOG: resultado dos filtros
        self._log(f"✅ Filtro aplicado. Físico: {len(self.df_fis)} | Contábil: {len(self.df_ctb)}")
        if self.df_fis is None or self.df_fis.empty:
            self._log("⚠️ Nenhuma referência encontrada no Físico para os filtros informados.")
        if self.df_ctb is None or self.df_ctb.empty:
            self._log("⚠️ Nenhuma referência encontrada no Contábil para os filtros informados.")
        elif mode == "Automático (02)" and auto_pairs and not self.pending_pairs:
            self._log("⚠️ Auto02 encontrou pares, mas não foi possível preparar o staging.", level="warn")

    def _stage_auto02_pairs(self, pairs: list[tuple[int, int]]) -> None:
        # Recria staging do zero para refletir exatamente o resultado atual do Auto02.
        self._clear_pending()
        if not pairs:
            return
        if self.df_fis is None or self.df_ctb is None or self.df_fis.empty or self.df_ctb.empty:
            return

        def _id_map(df: pd.DataFrame) -> dict[int, int]:
            out: dict[int, int] = {}
            for i, (_, r) in enumerate(df.iterrows()):
                try:
                    rid = int(float(r.get("ID", 0) or 0))
                except Exception:
                    rid = 0
                if rid > 0 and rid not in out:
                    out[rid] = i
            return out

        map_f = _id_map(self.df_fis)
        map_c = _id_map(self.df_ctb)

        self._batch_seq += 1
        batch = self._batch_seq
        added = 0

        for fis_id, ctb_id in pairs:
            row_f = map_f.get(int(fis_id))
            row_c = map_c.get(int(ctb_id))
            if row_f is None or row_c is None:
                continue
            if self._fis_id_count.get(int(fis_id), 0) > 0:
                continue
            if self._ctb_id_count.get(int(ctb_id), 0) > 0:
                continue

            self.pending_pairs.append(
                {
                    "fis_id": int(fis_id),
                    "ctb_id": int(ctb_id),
                    "role": "PAI",
                    "fis_row": int(row_f),
                    "ctb_row": int(row_c),
                    "batch": batch,
                }
            )
            self._fis_id_count[int(fis_id)] = self._fis_id_count.get(int(fis_id), 0) + 1
            self._ctb_id_count[int(ctb_id)] = self._ctb_id_count.get(int(ctb_id), 0) + 1
            self._fis_row_count[int(row_f)] = self._fis_row_count.get(int(row_f), 0) + 1
            self._ctb_row_count[int(row_c)] = self._ctb_row_count.get(int(row_c), 0) + 1

            self.tv_fis.item(str(row_f), tags=("PENDING",))
            self.tv_ctb.item(str(row_c), tags=("PENDING",))
            added += 1

        self._rebuild_pending_listbox()
        if added:
            self._log(f"🤖 Auto02 preparou {added} par(es) no staging.")

    # ---------- Data ----------
    # ---------- Data ----------
    # ---------- Data ----------
    # ---------- Data ----------
    # ---------- Data ----------
    def _refresh_counts_only(self):
        try:
            with connect(self.db_path) as con:
                c = get_counts(con)
            self.var_pf.set(str(c.fis))
            self.var_pc.set(str(c.ctb))
        except Exception:
            self.var_pf.set("0")
            self.var_pc.set("0")

    def _clear_tables(self):
        for tv in (self.tv_fis, self.tv_ctb):
            tv.delete(*tv.get_children())

    def _populate(self, tv: ttk.Treeview, df: pd.DataFrame, *, base: str):
        tv.delete(*tv.get_children())
        if df is None or df.empty:
            return

        def _val(x):
            try:
                if pd.isna(x):
                    return ""
            except Exception:
                pass
            return "" if x is None else x

        for i, (_, r) in enumerate(df.iterrows()):
            iid = str(i)

            if base == "FIS":
                values = (
                    int(_val(r.get("ID", 0)) or 0),
                    _val(r.get("FILIAL", "")),
                    _val(r.get("CCUSTO", "")),
                    _val(r.get("LOCAL", "")),
                    _val(r.get("CONDIC", "")),
                    _val(r.get("NRBRM", "")),
                    _val(r.get("INC", "")),
                    _clean_cell(_val(r.get("DESCRICAO", ""))),
                    _val(r.get("MARCA", "")),
                    _val(r.get("MODELO", "")),
                    _val(r.get("SERIE", "")),
                    _val(r.get("DIMENSAO", "")),
                    _val(r.get("CAPACIDADE", "")),
                    _val(r.get("TAG", "")),
                )
            else:
                dt = _val(r.get("DT_AQUISICAO", ""))
                dt = "" if dt is None else str(dt)
                if len(dt) > 10:
                    dt = dt[:10]
                values = (
                    int(_val(r.get("ID", 0)) or 0),
                    _val(r.get("FILIAL", "")),
                    _val(r.get("CCUSTO", "")),
                    _val(r.get("LOCAL", "")),
                    _val(r.get("NRBRM", "")),
                    _val(r.get("INC", "")),
                    _clean_cell(_val(r.get("DESCRICAO", ""))),
                    _val(r.get("SERIE", "")),
                    _val(r.get("TAG", "")),
                    dt,
                    _val(r.get("VLR_AQUISICAO", "")),
                    _val(r.get("DEP_ACUMULADA", "")),
                    _val(r.get("VLR_RESIDUAL", "")),
                )

            tv.insert("", "end", iid=iid, values=values)

        try:
            self._autosize_columns(tv)
        except Exception:
            pass

    # ---------- Pending panel helpers ----------
    def _pending_label(self, item: dict) -> str:
        fis_id = item["fis_id"]
        ctb_id = item["ctb_id"]
        role = item.get("role", "")
        suf = f" ({role})" if role else ""
        return f"FIS {fis_id}  →  CTB {ctb_id}{suf}"

    def _rebuild_pending_listbox(self):
        self.lb_pending.delete(0, tk.END)
        for it in self.pending_pairs:
            self.lb_pending.insert(tk.END, self._pending_label(it))
        self.var_pp.set(str(len(self.pending_pairs)))

    def _clear_pending(self):
        # despinta tudo
        for tv in (self.tv_fis, self.tv_ctb):
            for iid in tv.get_children():
                tv.item(iid, tags=())

        self.pending_pairs.clear()
        self._fis_id_count.clear()
        self._ctb_id_count.clear()
        self._fis_row_count.clear()
        self._ctb_row_count.clear()

        self._rebuild_pending_listbox()
        self._log("🧹 Pares pendentes limpos (mantidos os filtros).")


    def _undo_last_pending(self):
        """Desfaz o último pareamento criado (último clique), incluindo incorporações (FILHO) do mesmo lote."""
        if not self.pending_pairs:
            self._log("⚠️ Não há pares pendentes para desfazer.", level="warn")
            return

        # identifica o último lote
        try:
            last_batch = max(int(it.get("batch", 0) or 0) for it in self.pending_pairs)
        except Exception:
            last_batch = 0

        if last_batch == 0:
            # fallback: remove apenas o último item
            to_remove = [self.pending_pairs[-1]]
        else:
            to_remove = [it for it in self.pending_pairs if int(it.get("batch", 0) or 0) == last_batch]

        # remove do fim para não bagunçar índices (não dependemos de índice, mas é mais seguro)
        removed_count = 0
        for it in list(to_remove):
            if it not in self.pending_pairs:
                continue
            self.pending_pairs.remove(it)
            removed_count += 1

            fis_id = int(it.get("fis_id") or 0)
            ctb_id = int(it.get("ctb_id") or 0)
            fis_row = int(it.get("fis_row") or 0)
            ctb_row = int(it.get("ctb_row") or 0)

            # decrementa contadores por ID
            if fis_id:
                self._fis_id_count[fis_id] = max(0, self._fis_id_count.get(fis_id, 0) - 1)
                if self._fis_id_count[fis_id] == 0:
                    self._fis_id_count.pop(fis_id, None)
            if ctb_id:
                self._ctb_id_count[ctb_id] = max(0, self._ctb_id_count.get(ctb_id, 0) - 1)
                if self._ctb_id_count[ctb_id] == 0:
                    self._ctb_id_count.pop(ctb_id, None)

            # decrementa contadores por ROW (para manter o destaque azul enquanto houver algo pendente)
            self._fis_row_count[fis_row] = max(0, self._fis_row_count.get(fis_row, 0) - 1)
            if self._fis_row_count.get(fis_row, 0) == 0:
                self._fis_row_count.pop(fis_row, None)
                # despinta a linha do físico
                try:
                    self.tv_fis.item(str(fis_row), tags=())
                except Exception:
                    pass

            self._ctb_row_count[ctb_row] = max(0, self._ctb_row_count.get(ctb_row, 0) - 1)
            if self._ctb_row_count.get(ctb_row, 0) == 0:
                self._ctb_row_count.pop(ctb_row, None)
                # despinta a linha do contábil
                try:
                    self.tv_ctb.item(str(ctb_row), tags=())
                except Exception:
                    pass

        self._rebuild_pending_listbox()

        if removed_count <= 0:
            self._log("⚠️ Nenhum par foi desfeito.", level="warn")
        else:
            self._log(f"↩️ Desfeito(s) {removed_count} par(es) do último clique. Pendentes: {len(self.pending_pairs)}")


    def _remove_selected_pending(self):
        sel = self.lb_pending.curselection()
        if not sel:
            return
        idx = int(sel[0])
        if idx < 0 or idx >= len(self.pending_pairs):
            return

        it = self.pending_pairs.pop(idx)
        fis_id = int(it["fis_id"])
        ctb_id = int(it["ctb_id"])
        fis_row = int(it["fis_row"])
        ctb_row = int(it["ctb_row"])

        # decrementa contadores de IDs
        self._fis_id_count[fis_id] = max(0, self._fis_id_count.get(fis_id, 0) - 1)
        if self._fis_id_count[fis_id] == 0:
            del self._fis_id_count[fis_id]

        self._ctb_id_count[ctb_id] = max(0, self._ctb_id_count.get(ctb_id, 0) - 1)
        if self._ctb_id_count[ctb_id] == 0:
            del self._ctb_id_count[ctb_id]

        # decrementa contadores de linhas pintadas
        self._fis_row_count[fis_row] = max(0, self._fis_row_count.get(fis_row, 0) - 1)
        if self._fis_row_count.get(fis_row, 0) == 0:
            self._fis_row_count.pop(fis_row, None)
            if self.tv_fis.exists(str(fis_row)):
                self.tv_fis.item(str(fis_row), tags=())

        self._ctb_row_count[ctb_row] = max(0, self._ctb_row_count.get(ctb_row, 0) - 1)
        if self._ctb_row_count.get(ctb_row, 0) == 0:
            self._ctb_row_count.pop(ctb_row, None)
            if self.tv_ctb.exists(str(ctb_row)):
                self.tv_ctb.item(str(ctb_row), tags=())

        self._rebuild_pending_listbox()

    # ---------- Pair actions ----------
    def _create_pair(self):
        sel_f = self.tv_fis.selection()
        sel_c = self.tv_ctb.selection()
        if not sel_f or not sel_c:
            messagebox.showwarning("Atenção", "Selecione 1 item do Físico e 1 item do Contábil.")
            return
        row_f = int(sel_f[0])
        row_c = int(sel_c[0])
        self._create_pair_rows(row_f, row_c, silent=False)

    def _save_pairs(self):
        if not self.pending_pairs:
            messagebox.showinfo("Conciliar", "Não há pares pendentes para conciliar.")
            return

        try:
            with connect(self.db_path) as con:
                pairs_ids = [(int(it["fis_id"]), int(it["ctb_id"])) for it in self.pending_pairs]
                saved = save_manual_pairs(con, pairs_ids)
        except Exception as e:
            messagebox.showerror("Erro", str(e))
            return

        messagebox.showinfo("Conciliar","Conciliação realizada com sucesso.",parent=self)
        self.lift()
        self.focus_force()

        self._log(f"✅ Conciliação concluída. Pares conciliados: {saved}")
        self._clear_pending()
        self._apply_filters()






if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", default="conciliador_v2.db")
    args = p.parse_args()

    root = tk.Tk()
    root.withdraw()
    ManualV2Window(root, db_path=args.db)
    root.mainloop()
