# depara_import_v1.py
from __future__ import annotations

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import pandas as pd

# IMPORTANTE: manual_db_v2_fixed.py deve expor save_direct_pairs (DIRETA) e save_nao_chapeavel_pairs (NÃO CHAPEÁVEL)
from manual_db_v2_fixed import connect, save_direct_pairs, save_nao_chapeavel_pairs, find_children_ctb_ids

BG = "#225781"

# Colunas aceitas (variações)
FIS_ID_COLS = ("ID_FIS", "ID_FISICO", "FIS_ID")
FIS_NRBEM_COLS = ("NRBEM_FIS", "NRBRM_FIS", "NRBRM_FISICO", "FIS_NRBEM", "FIS_NRBRM")
FIS_INC_COLS = ("INC_FIS", "FIS_INC", "INC_FISICO")

CTB_ID_COLS = ("ID_CTB", "ID_CONT", "ID_CONTABIL", "CTB_ID")
CTB_NRBEM_COLS = ("NRBEM_CTB", "NRBRM_CTB", "NRBRM_CONTABIL", "CTB_NRBEM", "CTB_NRBRM")
CTB_INC_COLS = ("INC_CTB", "INC_CTBN", "CTB_INC", "INC_CONTABIL")


def _first_col(df: pd.DataFrame, options: tuple[str, ...]) -> str | None:
    cols_upper = {str(c).strip().upper(): str(c).strip() for c in df.columns}
    for opt in options:
        if opt.upper() in cols_upper:
            return cols_upper[opt.upper()]
    return None


def _to_int(x) -> int | None:
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _is_blank(x, *, allow_zero: bool = False) -> bool:
    if x is None:
        return True
    if isinstance(x, str) and not x.strip():
        return True
    if not allow_zero and (x == 0 or x == "0"):
        return True
    return False


def _trio_any(id_v, nr_v, inc_v) -> bool:
    # ID e NRBEM: zero/"0" não conta como preenchido
    return (not _is_blank(id_v, allow_zero=False)) or (not _is_blank(nr_v, allow_zero=False)) or (not _is_blank(inc_v, allow_zero=True))


def _trio_all(id_v, nr_v, inc_v) -> bool:
    # INC = 0 é válido (pai)
    return (not _is_blank(id_v, allow_zero=False)) and (not _is_blank(nr_v, allow_zero=False)) and (not _is_blank(inc_v, allow_zero=True))



class DeParaImportWindow(tk.Toplevel):
    """Importação direta via De-Para:

    Layout recomendado (sempre preencher o TRIO quando usar um lado):
      ID_FIS, NRBEM_FIS, INC_FIS, ID_CTB, NRBEM_CTB, INC_CTB

    Regras:
    - Se você preencher qualquer campo do lado FÍSICO => precisa preencher os 3 (ID/NRBEM/INC).
    - Se você preencher qualquer campo do lado CONTÁBIL => precisa preencher os 3 (ID/NRBEM/INC).
    - Pelo menos um lado deve estar completo.
    - Suporta "Não Chapeável":
        * sem Físico  => preenche só o trio do CTB (FIS vazio)
        * sem Contábil=> preenche só o trio do FIS (CTB vazio)
    - Se CTB for pai (INC=0), pergunta se inclui filhos (INC≠0) do mesmo NRBRM.
    - Salva tudo de uma vez em ST_CONCILIACAO = 'DIRETA' (grava em depara + marca conciliados).
    """

    def __init__(self, master: tk.Misc, db_path: str):
        super().__init__(master)
        self.db_path = db_path

        self.title("Importar De-Para (conciliação direta)")
        self.state("zoomed")
        self.configure(bg=BG)

        self.df_in = pd.DataFrame()
        self.preview_rows: list[dict] = []
        self.excel_path: str = ""

        self._build()

    def _build(self):
        top = tk.Frame(self, bg=BG)
        top.pack(fill="x", padx=16, pady=10)

        tk.Label(
            top,
            text="IMPORTAR DE-PARA (CONCILIAÇÃO DIRETA)",
            fg="white",
            bg=BG,
            font=("Arial", 14, "bold"),
        ).pack(anchor="w")

        row = tk.Frame(top, bg=BG)
        row.pack(fill="x", pady=(10, 0))

        tk.Button(row, text="Selecionar Excel De-Para", width=24, command=self._pick_excel).pack(side="left")
        tk.Button(row, text="Validar / Prévia", width=18, command=self._validate_preview).pack(side="left", padx=(10, 0))
        tk.Button(row, text="Executar e Salvar", width=18, command=self._execute_save).pack(side="left", padx=(10, 0))

        tk.Button(row, text="Fechar", width=12, command=self.destroy).pack(side="right")

        info = tk.Frame(self, bg=BG)
        info.pack(fill="x", padx=16, pady=(8, 0))

        self.lbl_file = tk.Label(info, text="Arquivo: (nenhum)", fg="white", bg=BG)
        self.lbl_file.pack(anchor="w")

        self.lbl_counts = tk.Label(info, text="Linhas lidas: 0 | Linhas válidas: 0", fg="white", bg=BG)
        self.lbl_counts.pack(anchor="w")
        self.lbl_err_types = tk.Label(
            info,
            text="Erros: inconsistência=0 | já conciliado=0 | não encontrado=0 | incompleto=0 | sem lado=0 | outros=0",
            fg="white",
            bg=BG,
        )
        self.lbl_err_types.pack(anchor="w")

        # Preview
        mid = tk.Frame(self, bg=BG)
        mid.pack(fill="both", expand=True, padx=16, pady=10)

        cols = ("status", "fis_id", "ctb_id", "nrbrm_ctb", "inc_ctb", "obs")
        self.tv = ttk.Treeview(mid, columns=cols, show="headings", height=20)
        for c in cols:
            self.tv.heading(c, text=c)
        self.tv.column("status", width=90, anchor="center", stretch=False)
        self.tv.column("fis_id", width=90, anchor="e", stretch=False)
        self.tv.column("ctb_id", width=90, anchor="e", stretch=False)
        self.tv.column("nrbrm_ctb", width=110, anchor="e", stretch=False)
        self.tv.column("inc_ctb", width=90, anchor="e", stretch=False)
        self.tv.column("obs", width=900, anchor="w")

        self.tv.grid(row=0, column=0, sticky="nsew")
        mid.grid_rowconfigure(0, weight=1)
        mid.grid_columnconfigure(0, weight=1)

        sb = tk.Scrollbar(mid, orient="vertical", command=self.tv.yview, width=16, bd=1, relief="sunken")
        sb.grid(row=0, column=1, sticky="ns")
        self.tv.configure(yscrollcommand=sb.set)

        self.tv.tag_configure("OK", background="#1b5e20", foreground="white")
        self.tv.tag_configure("ERR", background="#7f1d1d", foreground="white")

    def _pick_excel(self):
        path = filedialog.askopenfilename(
            title="Selecione o Excel de De-Para",
            filetypes=[("Excel", "*.xlsx *.xlsm *.xls")],
        )
        if not path:
            return
        self.excel_path = path
        self.lbl_file.configure(text=f"Arquivo: {path}")
        self.df_in = pd.DataFrame()
        self.preview_rows = []
        self._refresh_preview()
        self.lbl_counts.configure(text="Linhas lidas: 0 | Linhas válidas: 0")
        self.lbl_err_types.configure(
            text="Erros: inconsistência=0 | já conciliado=0 | não encontrado=0 | incompleto=0 | sem lado=0 | outros=0"
        )

    def _read_excel(self) -> pd.DataFrame:
        if not self.excel_path:
            raise ValueError("Selecione um arquivo Excel primeiro.")
        df = pd.read_excel(self.excel_path, dtype=str)
        if df is None or df.empty:
            return pd.DataFrame()
        df.columns = [str(c).strip() for c in df.columns]
        return df

    def _resolve_fis_id(self, con, id_fis: int | None, nrbrm: int | None, inc: int | None) -> tuple[int | None, str | None]:
        cur = con.cursor()
        if id_fis is None:
            return None, "Físico: ID_FIS inválido."
        if nrbrm is None:
            return None, "Físico: NRBEM_FIS inválido."

        inc = 0 if inc is None else int(inc)

        row = cur.execute(
            """
            SELECT f.ID, COALESCE(f.NRBRM,0), COALESCE(f.INC,0),
                   CASE WHEN c.ID IS NULL THEN 0 ELSE 1 END AS JA_CONCILIADO
            FROM fisico f
            LEFT JOIN conciliados c ON c.BASE='FIS' AND c.ID=f.ID
            WHERE f.ID=?
            LIMIT 1
            """,
            (int(id_fis),),
        ).fetchone()

        if not row:
            alt = cur.execute(
                """
                SELECT ID
                FROM fisico
                WHERE COALESCE(NRBRM,0)=? AND COALESCE(INC,0)=?
                ORDER BY ID
                LIMIT 1
                """,
                (int(nrbrm), int(inc)),
            ).fetchone()
            if alt:
                return None, f"Físico: ID {int(id_fis)} não corresponde ao NRBEM/INC informado(s) (ID esperado: {int(alt[0])})."
            return None, f"Físico: ID {int(id_fis)} não encontrado."

        rid, rnr, rinc, ja_conc = int(row[0]), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
        if ja_conc:
            return None, f"Físico: ID {rid} já conciliado."
        if rnr != int(nrbrm) or rinc != int(inc):
            return None, f"Físico: ID {rid} não confere com NRBEM/INC informado(s)."
        return rid, None

    def _resolve_ctb_id(self, con, id_ctb: int | None, nrbrm: int | None, inc: int | None) -> tuple[int | None, str | None]:
        cur = con.cursor()
        if id_ctb is None:
            return None, "Contábil: ID_CTB inválido."
        if nrbrm is None:
            return None, "Contábil: NRBEM_CTB inválido."

        inc = 0 if inc is None else int(inc)

        row = cur.execute(
            """
            SELECT t.ID, COALESCE(t.NRBRM,0), COALESCE(t.INC,0),
                   CASE WHEN c.ID IS NULL THEN 0 ELSE 1 END AS JA_CONCILIADO
            FROM contabil t
            LEFT JOIN conciliados c ON c.BASE='CTB' AND c.ID=t.ID
            WHERE t.ID=?
            LIMIT 1
            """,
            (int(id_ctb),),
        ).fetchone()

        if not row:
            alt = cur.execute(
                """
                SELECT ID
                FROM contabil
                WHERE COALESCE(NRBRM,0)=? AND COALESCE(INC,0)=?
                ORDER BY ID
                LIMIT 1
                """,
                (int(nrbrm), int(inc)),
            ).fetchone()
            if alt:
                return None, f"Contábil: ID {int(id_ctb)} não corresponde ao NRBEM/INC informado(s) (ID esperado: {int(alt[0])})."
            return None, f"Contábil: ID {int(id_ctb)} não encontrado."

        rid, rnr, rinc, ja_conc = int(row[0]), int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)
        if ja_conc:
            return None, f"Contábil: ID {rid} já conciliado."
        if rnr != int(nrbrm) or rinc != int(inc):
            return None, f"Contábil: ID {rid} não confere com NRBEM/INC informado(s)."
        return rid, None

    def _get_ctb_nrbrm_inc(self, con, ctb_id: int) -> tuple[int | None, int | None]:
        cur = con.cursor()
        row = cur.execute("SELECT NRBRM, INC FROM contabil WHERE ID=?", (int(ctb_id),)).fetchone()
        if not row:
            return None, None
        return _to_int(row[0]), _to_int(row[1])

    def _validate_preview(self):
        try:
            df = self._read_excel()
        except Exception as e:
            messagebox.showerror("Erro", str(e), parent=self)
            return

        self.df_in = df
        self.preview_rows = []
        self._refresh_preview()

        if df.empty:
            self.lbl_counts.configure(text="Linhas lidas: 0 | Linhas válidas: 0")
            self.lbl_err_types.configure(
                text="Erros: inconsistência=0 | já conciliado=0 | não encontrado=0 | incompleto=0 | sem lado=0 | outros=0"
            )
            return

        # map cols
        c_id_fis = _first_col(df, FIS_ID_COLS)
        c_nr_fis = _first_col(df, FIS_NRBEM_COLS)
        c_inc_fis = _first_col(df, FIS_INC_COLS)

        c_id_ctb = _first_col(df, CTB_ID_COLS)
        c_nr_ctb = _first_col(df, CTB_NRBEM_COLS)
        c_inc_ctb = _first_col(df, CTB_INC_COLS)

        # Aqui exigimos ao menos 1 trio completo, então precisamos localizar colunas para ambos os lados.
        if not (c_id_fis and c_nr_fis and c_inc_fis):
            # permite mesmo assim: se o usuário quiser só CTB (Não Chapeável sem Físico), tudo bem — mas precisa das 3 do CTB.
            pass
        if not (c_id_ctb and c_nr_ctb and c_inc_ctb):
            pass

        ok = 0
        err_stats = {
            "inconsistencia": 0,
            "ja_conciliado": 0,
            "nao_encontrado": 0,
            "incompleto": 0,
            "sem_lado": 0,
            "outros": 0,
        }
        with connect(self.db_path) as con:
            for i, row in df.iterrows():
                id_fis_in = _to_int(row.get(c_id_fis)) if c_id_fis else None
                nr_fis_in = _to_int(row.get(c_nr_fis)) if c_nr_fis else None
                inc_fis_in = _to_int(row.get(c_inc_fis)) if c_inc_fis else None

                id_ctb_in = _to_int(row.get(c_id_ctb)) if c_id_ctb else None
                nr_ctb_in = _to_int(row.get(c_nr_ctb)) if c_nr_ctb else None
                inc_ctb_in = _to_int(row.get(c_inc_ctb)) if c_inc_ctb else None

                fis_any = _trio_any(id_fis_in, nr_fis_in, inc_fis_in)
                fis_all = _trio_all(id_fis_in, nr_fis_in, inc_fis_in)
                ctb_any = _trio_any(id_ctb_in, nr_ctb_in, inc_ctb_in)
                ctb_all = _trio_all(id_ctb_in, nr_ctb_in, inc_ctb_in)

                status = "OK"
                obs_parts: list[str] = []

                if fis_any and not fis_all:
                    status = "ERRO"
                    obs_parts.append("Físico: preencha ID_FIS, NRBEM_FIS e INC_FIS.")
                if ctb_any and not ctb_all:
                    status = "ERRO"
                    obs_parts.append("Contábil: preencha ID_CTB, NRBEM_CTB e INC_CTB.")

                if status == "OK" and (not fis_all and not ctb_all):
                    status = "ERRO"
                    obs_parts.append("Informe pelo menos um lado completo (Físico ou Contábil).")

                fis_id = 0
                ctb_id = 0

                if status == "OK" and fis_all:
                    resolved, msg = self._resolve_fis_id(con, id_fis_in, nr_fis_in, inc_fis_in)
                    if not resolved:
                        status = "ERRO"
                        obs_parts.append(msg or "Físico inválido.")
                    else:
                        fis_id = int(resolved)

                if status == "OK" and ctb_all:
                    resolved, msg = self._resolve_ctb_id(con, id_ctb_in, nr_ctb_in, inc_ctb_in)
                    if not resolved:
                        status = "ERRO"
                        obs_parts.append(msg or "Contábil inválido.")
                    else:
                        ctb_id = int(resolved)

                nrbrm_c, inc_c = (0, "")
                if status == "OK" and ctb_id:
                    nrbrm_tmp, inc_tmp = self._get_ctb_nrbrm_inc(con, ctb_id)
                    nrbrm_c = int(nrbrm_tmp or 0)
                    inc_c = "" if inc_tmp is None else int(inc_tmp)
                    if nrbrm_c and int(inc_c or 0) == 0:
                        try:
                            child_ids = find_children_ctb_ids(
                                con,
                                nrbrm=int(nrbrm_c),
                                exclude_ctb_id=int(ctb_id),
                                limit=2000,
                            )
                        except Exception:
                            child_ids = []
                        if child_ids:
                            obs_parts.append(
                                f"Pai CTB com {len(child_ids)} filho(s) pendente(s) (INC≠0); serão incluídos na conciliação."
                            )

                if status == "OK":
                    if ctb_id and not fis_id:
                        obs_parts.append("Não Chapeável (sem Físico)")
                    elif fis_id and not ctb_id:
                        obs_parts.append("Não Chapeável (sem Contábil)")

                    ok += 1
                else:
                    obs_txt = " ".join(obs_parts).lower()
                    has_any = False
                    if ("não confere" in obs_txt) or ("não corresponde" in obs_txt):
                        err_stats["inconsistencia"] += 1
                        has_any = True
                    if "já conciliado" in obs_txt:
                        err_stats["ja_conciliado"] += 1
                        has_any = True
                    if "não encontrado" in obs_txt:
                        err_stats["nao_encontrado"] += 1
                        has_any = True
                    if "preencha" in obs_txt:
                        err_stats["incompleto"] += 1
                        has_any = True
                    if "pelo menos um lado completo" in obs_txt:
                        err_stats["sem_lado"] += 1
                        has_any = True
                    if not has_any:
                        err_stats["outros"] += 1

                self.preview_rows.append(
                    dict(
                        idx=i,
                        status="OK" if status == "OK" else "ERRO",
                        fis_id=fis_id,
                        ctb_id=ctb_id,
                        nrbrm_ctb=nrbrm_c,
                        inc_ctb=inc_c,
                        obs=" ".join(obs_parts).strip(),
                    )
                )

        self.lbl_counts.configure(text=f"Linhas lidas: {len(df)} | Linhas válidas: {ok}")
        self.lbl_err_types.configure(
            text=(
                "Erros: "
                f"inconsistência={err_stats['inconsistencia']} | "
                f"já conciliado={err_stats['ja_conciliado']} | "
                f"não encontrado={err_stats['nao_encontrado']} | "
                f"incompleto={err_stats['incompleto']} | "
                f"sem lado={err_stats['sem_lado']} | "
                f"outros={err_stats['outros']}"
            )
        )
        self._refresh_preview()

    def _refresh_preview(self):
        self.tv.delete(*self.tv.get_children())
        for it in self.preview_rows:
            tag = "OK" if it["status"] == "OK" else "ERR"
            self.tv.insert(
                "",
                "end",
                values=(it["status"], it["fis_id"], it["ctb_id"], it["nrbrm_ctb"], it["inc_ctb"], it["obs"]),
                tags=(tag,),
            )

    def _execute_save(self):
        if not self.preview_rows:
            messagebox.showwarning("Atenção", "Clique em 'Validar / Prévia' primeiro.", parent=self)
            return

        ok_rows = [r for r in self.preview_rows if r["status"] == "OK"]
        if not ok_rows:
            messagebox.showwarning("Atenção", "Nenhuma linha válida para salvar.", parent=self)
            return

        pairs: list[tuple[int, int]] = []
        asked = 0
        included_children = 0

        with connect(self.db_path) as con:
            child_candidates: list[tuple[dict, int, int, int, list[int]]] = []
            for r in ok_rows:
                fis_id = int(r["fis_id"] or 0)
                ctb_id = int(r["ctb_id"] or 0)
                pairs.append((fis_id, ctb_id))

                # coleta filhos quando CTB existe e é PAI (INC=0)
                if ctb_id > 0:
                    nrbrm = _to_int(r["nrbrm_ctb"])
                    inc = _to_int(r["inc_ctb"])
                    if nrbrm and (inc == 0):
                        try:
                            child_ids = find_children_ctb_ids(
                                con,
                                nrbrm=int(nrbrm),
                                exclude_ctb_id=int(ctb_id),
                                limit=2000,
                            )
                        except Exception:
                            child_ids = []
                        if child_ids:
                            child_candidates.append((r, fis_id, ctb_id, int(nrbrm), [int(x) for x in child_ids]))

            # Estratégia global para incorporações
            include_mode = "none"  # none | all | one_by_one
            if child_candidates:
                total_children = sum(len(x[4]) for x in child_candidates)
                resp = messagebox.askyesnocancel(
                    "Incorporações",
                    "Foram encontradas incorporações (INC≠0).\n\n"
                    f"Itens pai: {len(child_candidates)}\n"
                    f"Filhos pendentes: {total_children}\n\n"
                    "Sim = conciliar TODAS as incorporações de uma vez.\n"
                    "Não = decidir UMA A UMA.\n"
                    "Cancelar = não incluir incorporações.",
                    parent=self,
                )
                if resp is True:
                    include_mode = "all"
                elif resp is False:
                    include_mode = "one_by_one"

            for r, fis_id, _ctb_id, nrbrm, child_ids in child_candidates:
                if include_mode == "all":
                    for cid in child_ids:
                        pairs.append((fis_id, int(cid)))
                        included_children += 1
                    continue

                if include_mode == "one_by_one":
                    asked += 1
                    resp = messagebox.askyesno(
                        "Incorporações",
                        f"Linha {r['idx']+1}: encontrei {len(child_ids)} filho(s) (INC≠0) para NrBrm {nrbrm}.\n\nConciliar também?",
                        parent=self,
                    )
                    if resp:
                        for cid in child_ids:
                            pairs.append((fis_id, int(cid)))
                            included_children += 1

        # remove duplicados, preserva ordem
        seen = set()
        pairs_unique: list[tuple[int, int]] = []
        for a, b in pairs:
            k = (int(a or 0), int(b or 0))
            if k in seen:
                continue
            seen.add(k)
            pairs_unique.append(k)

        try:
            with connect(self.db_path) as con:
                pairs_direta = [(f, c) for (f, c) in pairs_unique if int(f or 0) > 0 and int(c or 0) > 0]
                pairs_nc = [(f, c) for (f, c) in pairs_unique if ((int(f or 0) > 0) ^ (int(c or 0) > 0))]
                saved_direta = save_direct_pairs(con, pairs_direta) if pairs_direta else 0
                saved_nc = save_nao_chapeavel_pairs(con, pairs_nc) if pairs_nc else 0
                saved = saved_direta + saved_nc
        except Exception as e:
            messagebox.showerror("Erro", str(e), parent=self)
            return

        messagebox.showinfo(
            "OK",
            f"Pares salvos: {saved}\n"
            f"Filhos incluídos: {included_children}\n"
            f"Linhas perguntadas sobre filhos: {asked}\n"
            f"Total pares enviados: {len(pairs_unique)}",
            parent=self,
        )

        # limpa prévia
        self.preview_rows = []
        self._refresh_preview()
        self.lbl_counts.configure(text="Linhas lidas: 0 | Linhas válidas: 0")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--db", default="conciliador_v2.db")
    args = p.parse_args()

    root = tk.Tk()
    root.withdraw()
    DeParaImportWindow(root, db_path=args.db)
    root.mainloop()
