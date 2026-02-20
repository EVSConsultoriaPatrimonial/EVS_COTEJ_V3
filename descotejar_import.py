# descotejar_import.py
from __future__ import annotations

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from typing import List, Tuple

import pandas as pd

from manual_db_v2_fixed import connect, undo_pairs

BG = "#225781"

# Colunas aceitas (mesmas do De-Para)
FIS_ID_COLS = ("ID_FIS", "ID_FISICO", "FIS_ID", "fis_id", "fisico_id")
CTB_ID_COLS = ("ID_CTB", "ID_CONT", "ID_CONTABIL", "CTB_ID", "ctb_id", "contabil_id")

def _maximize(win: tk.Toplevel) -> None:
    try:
        win.state("zoomed")  # Windows
        return
    except Exception:
        pass
    try:
        win.attributes("-zoomed", True)  # alguns Tk no macOS/Linux
        return
    except Exception:
        pass
    try:
        w = win.winfo_screenwidth()
        h = win.winfo_screenheight()
        win.geometry(f"{w}x{h}+0+0")
    except Exception:
        pass

def _first_col(df: pd.DataFrame, options) -> str | None:
    cols = {str(c).strip().upper(): c for c in df.columns}
    for o in options:
        ou = str(o).strip().upper()
        if ou in cols:
            return cols[ou]
    return None

class DescotejarImportWindow(tk.Toplevel):
    """Importa planilha no layout do De-Para para DESCONCILIAR (descotejar) itens."""

    def __init__(self, master: tk.Misc, db_path: str):
        super().__init__(master)
        self.db_path = db_path

        self.title("Descotejar — Importar De-Para (desfazer conciliações)")
        self.configure(bg=BG)
        _maximize(self)

        self.file_path = tk.StringVar(value="")
        self.status = tk.StringVar(value="Selecione a planilha no layout do De-Para para descotejar.")
        self.df: pd.DataFrame | None = None
        self.valid_pairs: List[Tuple[int, int]] = []

        self._build()

    def _build(self):
        top = tk.Frame(self, bg=BG)
        top.pack(fill="x", padx=12, pady=(12, 6))

        tk.Label(
            top,
            text="DESCOTEJAR (DESCONCILIAR) — Importar Planilha",
            bg=BG, fg="white",
            font=("Helvetica", 14, "bold")
        ).pack(side="left")

        btns = tk.Frame(top, bg=BG)
        btns.pack(side="right")

        tk.Button(btns, text="Selecionar Excel De-Para", width=22, command=self._pick).pack(side="left", padx=6)
        tk.Button(btns, text="Validar / Prévia", width=16, command=self._validate).pack(side="left", padx=6)
        tk.Button(btns, text="Executar e Descotejar", width=20, command=self._execute).pack(side="left", padx=6)
        tk.Button(btns, text="Fechar", width=12, command=self.destroy).pack(side="left", padx=6)

        info = tk.Frame(self, bg=BG)
        info.pack(fill="x", padx=12, pady=(0, 10))
        tk.Label(info, textvariable=self.file_path, bg=BG, fg="white", font=("Helvetica", 10)).pack(anchor="w")
        tk.Label(info, textvariable=self.status, bg=BG, fg="#cfe8e8", font=("Helvetica", 10, "bold")).pack(anchor="w")

        # tabela prévia
        mid = tk.Frame(self, bg=BG)
        mid.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        cols = ("status", "fis_id", "ctb_id", "obs")
        self.tree = ttk.Treeview(mid, columns=cols, show="headings", height=24)
        for c in cols:
            self.tree.heading(c, text=c)
            if c == "obs":
                self.tree.column(c, width=560, anchor="w")
            else:
                self.tree.column(c, width=180, anchor="center")
        self.tree.pack(side="left", fill="both", expand=True)

        sb = ttk.Scrollbar(mid, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")

    def _clear_tree(self):
        for iid in self.tree.get_children():
            self.tree.delete(iid)

    def _pick(self):
        p = filedialog.askopenfilename(
            title="Selecionar Excel (layout do De-Para)",
            filetypes=[("Excel", "*.xlsx *.xls")],
        )
        if not p:
            return
        self.file_path.set(f"Arquivo: {p}")
        self.status.set("Arquivo selecionado. Clique em 'Validar / Prévia'.")
        self.df = None
        self.valid_pairs = []
        self._clear_tree()

        try:
            self.df = pd.read_excel(p)
        except Exception as e:
            messagebox.showerror("Erro", f"Falha ao ler Excel:\n{e}")
            self.df = None

    def _validate(self):
        if self.df is None:
            messagebox.showwarning("Descotejar", "Selecione uma planilha primeiro.")
            return

        df = self.df.copy()
        fis_col = _first_col(df, FIS_ID_COLS)
        ctb_col = _first_col(df, CTB_ID_COLS)

        if not fis_col and not ctb_col:
            messagebox.showerror(
                "Descotejar",
                "Não encontrei as colunas de ID.\n\n"
                "Aceitas (um lado já basta):\n"
                f"- Físico: {', '.join(FIS_ID_COLS)}\n"
                f"- Contábil: {', '.join(CTB_ID_COLS)}"
            )
            return

        def as_int(v):
            try:
                if pd.isna(v):
                    return 0
                return int(float(v))
            except Exception:
                return 0

        pairs: List[Tuple[int, int]] = []
        preview = []

        for _, row in df.iterrows():
            fis_id = as_int(row[fis_col]) if fis_col else 0
            ctb_id = as_int(row[ctb_col]) if ctb_col else 0
            if fis_id <= 0 and ctb_id <= 0:
                continue
            pairs.append((fis_id, ctb_id))
            preview.append(("OK", fis_id, ctb_id, ""))

        self.valid_pairs = pairs
        self._clear_tree()

        for st, fis_id, ctb_id, obs in preview[:2000]:
            self.tree.insert("", "end", values=(st, fis_id, ctb_id, obs))

        self.status.set(f"Linhas válidas: {len(pairs)} (prévia exibindo até 2000)")

    def _execute(self):
        if not self.valid_pairs:
            messagebox.showwarning("Descotejar", "Nada para descotejar. Faça 'Validar / Prévia' primeiro.")
            return

        if not messagebox.askyesno(
            "Confirmar",
            "Isso vai DESCONCILIAR os itens informados (remover do De-Para e desbloquear no banco).\n\n"
            "Deseja continuar?"
        ):
            return

        try:
            con = connect(self.db_path)
            try:
                res = undo_pairs(con, self.valid_pairs)
            finally:
                con.close()

            messagebox.showinfo(
                "Descotejar",
                "Processo concluído!\n\n"
                f"Removidos De-Para: {res.get('removed_depara', 0)}\n"
                f"Removidos bloqueios (conciliados): {res.get('removed_conc', 0)}\n"
                f"Desmarcados FRAG físico: {res.get('unmarked_fis', 0)}\n"
                f"Desmarcados FRAG contábil: {res.get('unmarked_ctb', 0)}"
            )
        except Exception as e:
            messagebox.showerror("Descotejar", f"Falha ao descotejar:\n{e}")
