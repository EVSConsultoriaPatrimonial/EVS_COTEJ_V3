# matcher_v2_extended.py (PG + SQLite compatível)
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

import pandas as pd

from db_utils_v2 import connect, init_db


@dataclass
class MatchStats:
    st: str
    candidatos_fisico: int
    candidatos_contabil: int
    conciliados: int


# -----------------------------
# Engine helpers
# -----------------------------
def _is_postgres(conn) -> bool:
    return conn.__class__.__module__.startswith("psycopg2")


def _table_columns(conn, table: str) -> set[str]:
    cur = conn.cursor()
    if _is_postgres(conn):
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            """,
            (table.lower(),),
        )
        return {r[0] for r in cur.fetchall()}
    else:
        cur.execute(f"PRAGMA table_info({table});")
        return {r[1] for r in cur.fetchall()}


def _col(conn, table: str, preferred: str, fallbacks: Tuple[str, ...]) -> str:
    cols = _table_columns(conn, table)
    if preferred in cols:
        return preferred
    for fb in fallbacks:
        if fb in cols:
            return fb
    return preferred  # last resort (will error clearly)


def _qident(name: str) -> str:
    # safe identifier quoting for PG; works fine in SQLite too
    return f'"{name}"'


def _paramstyle(conn) -> str:
    return "%s" if _is_postgres(conn) else "?"


def _read_pending(conn, table: str, base: str, cols: list[str]) -> pd.DataFrame:
    """
    Lê registros ainda não conciliados:
    - Preferência: checar por tabela 'conciliados' (base/base_id ou BASE/ID).
    - Fallback: checar FRAG != 'Conciliado' quando existir.
    """
    t_id = _col(conn, table, "id", ("ID",))
    t_frag = _col(conn, table, "frag", ("FRAG",))
    has_frag = t_frag in _table_columns(conn, table)

    c_base = _col(conn, "conciliados", "base", ("BASE",))
    c_base_id = _col(conn, "conciliados", "base_id", ("ID", "baseid", "BASE_ID"))
    # build select list
    select_list = ", ".join([f"t.{_qident(_col(conn, table, c, (c.upper(),)))} AS {_qident(c)}" for c in cols])

    p = _paramstyle(conn)
    where_frag = ""
    params = [base]
    if has_frag:
        where_frag = f" AND COALESCE(t.{_qident(t_frag)}, '') <> 'Conciliado' "

    sql = f"""
    SELECT {select_list}
    FROM {_qident(table)} t
    WHERE t.{_qident(t_id)} IS NOT NULL
      {where_frag}
      AND NOT EXISTS (
        SELECT 1 FROM {_qident('conciliados')} c
        WHERE c.{_qident(c_base)} = {p}
          AND c.{_qident(c_base_id)} = t.{_qident(t_id)}
      )
    """
    df = pd.read_sql_query(sql, conn, params=tuple(params))
    # normalize ID to int
    if "ID" in df.columns:
        df["ID"] = pd.to_numeric(df["ID"], errors="coerce").astype("Int64")
        df = df.dropna(subset=["ID"])
    return df


def _pair_1to1_by_value(df_f: pd.DataFrame, df_c: pd.DataFrame, left_key: str, right_key: str) -> pd.DataFrame:
    df_f = df_f.copy()
    df_c = df_c.copy()

    df_f["_rk"] = df_f.groupby(left_key)["ID"].rank(method="first").astype(int)
    df_c["_rk"] = df_c.groupby(right_key)["ID"].rank(method="first").astype(int)

    m = df_f.merge(df_c, left_on=[left_key, "_rk"], right_on=[right_key, "_rk"], suffixes=("_F", "_C"))
    # standard output
    out = pd.DataFrame(
        {
            "ID_FISICO": m["ID_F"],
            "ID_CONTABIL": m["ID_C"],
            "NRBRM": m.get("NRBRM_F", m.get("NRBRM", 0)),
            "INC_CONTABIL": m.get("INC_C", m.get("INC", 0)),
        }
    )
    return out


def _next_par_id(conn) -> int:
    # SQLite: MAX(PAR_ID)+1; PG: MAX(par_id)+1 (both work with quoting)
    cur = conn.cursor()
    cols = _table_columns(conn, "depara")
    par_col = "par_id" if "par_id" in cols else ("PAR_ID" if "PAR_ID" in cols else "par_id")
    cur.execute(f"SELECT COALESCE(MAX({_qident(par_col)}), 0) + 1 FROM {_qident('depara')};")
    return int(cur.fetchone()[0] or 1)


def _bulk_insert_pairs(conn, pairs: pd.DataFrame, st: str) -> int:
    if pairs.empty:
        return 0

    init_db(conn)

    cols_depara = _table_columns(conn, "depara")
    # choose target columns
    par_col = "par_id" if "par_id" in cols_depara else "PAR_ID"
    st_col = "st_conciliacao" if "st_conciliacao" in cols_depara else "ST_CONCILIACAO"
    idf_col = "id_fisico" if "id_fisico" in cols_depara else "ID_FISICO"
    idc_col = "id_contabil" if "id_contabil" in cols_depara else "ID_CONTABIL"
    nr_col = "nrbrm" if "nrbrm" in cols_depara else "NRBRM"
    inc_col = "inc_contabil" if "inc_contabil" in cols_depara else "INC_CONTABIL"

    cols_conc = _table_columns(conn, "conciliados")
    c_base = "base" if "base" in cols_conc else "BASE"
    c_base_id = "base_id" if "base_id" in cols_conc else "ID"
    c_par = "par_id" if "par_id" in cols_conc else ("PAR_ID" if "PAR_ID" in cols_conc else "par_id")

    start = _next_par_id(conn)
    pstyle = _paramstyle(conn)
    cur = conn.cursor()

    # build rows
    pairs = pairs.dropna(subset=["ID_CONTABIL"]).copy()
    pairs["ID_FISICO"] = pd.to_numeric(pairs["ID_FISICO"], errors="coerce").fillna(0)
    pairs["NRBRM"] = pd.to_numeric(pairs["NRBRM"], errors="coerce").fillna(0).astype(int)
    pairs["INC_CONTABIL"] = pd.to_numeric(pairs["INC_CONTABIL"], errors="coerce").fillna(0).astype(int)
    pairs["ID_FISICO"] = pd.to_numeric(pairs["ID_FISICO"], errors="coerce").fillna(0).astype(int)
    pairs["ID_CONTABIL"] = pd.to_numeric(pairs["ID_CONTABIL"], errors="coerce").astype(int)

    depara_rows = []
    conc_rows = []
    for i, r in enumerate(pairs.itertuples(index=False)):
        par = start + i
        depara_rows.append((par, st, int(r.ID_FISICO), int(r.ID_CONTABIL), int(r.NRBRM), int(r.INC_CONTABIL)))
        if int(r.ID_FISICO) > 0:
            conc_rows.append(("FIS", int(r.ID_FISICO), par))
        conc_rows.append(("CTB", int(r.ID_CONTABIL), par))

    # insert depara
    ins_depara = f"""
    INSERT INTO {_qident('depara')} ({_qident(par_col)}, {_qident(st_col)}, {_qident(idf_col)}, {_qident(idc_col)}, {_qident(nr_col)}, {_qident(inc_col)})
    VALUES ({pstyle},{pstyle},{pstyle},{pstyle},{pstyle},{pstyle})
    """
    cur.executemany(ins_depara, depara_rows)

        # insert conciliados (evita UNIQUE constraint BASE+ID)
    if _is_postgres(conn):
        ins_conc = f"""
        INSERT INTO {_qident('conciliados')} ({_qident(c_base)}, {_qident(c_base_id)}, {_qident(c_par)})
        VALUES ({pstyle},{pstyle},{pstyle})
        ON CONFLICT ({_qident(c_base)}, {_qident(c_base_id)}) DO NOTHING
        """
    else:
        ins_conc = f"""
        INSERT OR IGNORE INTO {_qident('conciliados')} ({_qident(c_base)}, {_qident(c_base_id)}, {_qident(c_par)})
        VALUES ({pstyle},{pstyle},{pstyle})
        """
    cur.executemany(ins_conc, conc_rows)

    # also mark FRAG when present (helps export/visual)
    for table, base, ids in (("fisico", "FIS", [i for i in pairs["ID_FISICO"].tolist() if int(i) > 0]), ("contabil", "CTB", pairs["ID_CONTABIL"].tolist())):
        tcols = _table_columns(conn, table)
        frag = "frag" if "frag" in tcols else ("FRAG" if "FRAG" in tcols else None)
        tid = "id" if "id" in tcols else ("ID" if "ID" in tcols else "id")
        if frag:
            # UPDATE ... WHERE id IN (...)
            if not ids:
                continue
            # chunk to avoid huge SQL
            for k in range(0, len(ids), 5000):
                chunk = ids[k:k+5000]
                placeholders = ",".join([pstyle]*len(chunk))
                sql_upd = f"UPDATE {_qident(table)} SET {_qident(frag)}='Conciliado' WHERE {_qident(tid)} IN ({placeholders});"
                cur.execute(sql_upd, tuple(chunk))

    conn.commit()
    return len(pairs)


# =========================================================
# Regras usadas no Automático 01
# =========================================================
def run_regra_nrbrm_pai(db_path: str) -> MatchStats:
    st = "NRBEM_FIS=NRBEM_CTB"
    con = connect(db_path)
    try:
        init_db(con)

        df_f = _read_pending(con, "fisico", "FIS", ["ID", "NRBRM"])
        df_c = _read_pending(con, "contabil", "CTB", ["ID", "NRBRM", "INC"])

        cand_f, cand_c = len(df_f), len(df_c)

        df_f["NRBRM"] = pd.to_numeric(df_f["NRBRM"], errors="coerce")
        df_c["NRBRM"] = pd.to_numeric(df_c["NRBRM"], errors="coerce")
        df_c["INC"] = pd.to_numeric(df_c["INC"], errors="coerce").fillna(0).astype(int)

        df_f = df_f.dropna(subset=["NRBRM"])
        df_c = df_c.dropna(subset=["NRBRM"])
        df_c = df_c.loc[df_c["INC"] == 0].copy()

        if df_f.empty or df_c.empty:
            return MatchStats(st, cand_f, cand_c, 0)

        pairs = _pair_1to1_by_value(df_f, df_c, "NRBRM", "NRBRM")
        pairs["INC_CONTABIL"] = 0
        created = _bulk_insert_pairs(con, pairs, st)
        return MatchStats(st, cand_f, cand_c, int(created))
    finally:
        try:
            con.close()
        except Exception:
            pass


def run_regra_bem_ant_fis_eq_nrbrm_ctb(db_path: str) -> MatchStats:
    st = "BEMANT_FIS=NRBEM_CTB"
    con = connect(db_path)
    try:
        init_db(con)
        df_f = _read_pending(con, "fisico", "FIS", ["ID", "BEM_ANTERIOR"])
        df_c = _read_pending(con, "contabil", "CTB", ["ID", "NRBRM", "INC"])
        cand_f, cand_c = len(df_f), len(df_c)

        df_f["BEM_ANTERIOR"] = pd.to_numeric(df_f["BEM_ANTERIOR"], errors="coerce")
        df_c["NRBRM"] = pd.to_numeric(df_c["NRBRM"], errors="coerce")
        df_c["INC"] = pd.to_numeric(df_c["INC"], errors="coerce").fillna(0).astype(int)

        df_f = df_f.dropna(subset=["BEM_ANTERIOR"])
        df_c = df_c.dropna(subset=["NRBRM"])
        df_c = df_c.loc[df_c["INC"] == 0].copy()

        if df_f.empty or df_c.empty:
            return MatchStats(st, cand_f, cand_c, 0)

        pairs = _pair_1to1_by_value(df_f.rename(columns={"BEM_ANTERIOR":"KEY"}), df_c.rename(columns={"NRBRM":"KEY"}), "KEY", "KEY")
        pairs["NRBRM"] = pd.to_numeric(pairs["NRBRM"], errors="coerce").fillna(0).astype(int)
        pairs["INC_CONTABIL"] = 0
        created = _bulk_insert_pairs(con, pairs, st)
        return MatchStats(st, cand_f, cand_c, int(created))
    finally:
        try: con.close()
        except Exception: pass


def run_regra_nrbrm_fis_eq_bem_ant_ctb(db_path: str) -> MatchStats:
    st = "NRBRM_FIS=NRBEM_CTB"
    con = connect(db_path)
    try:
        init_db(con)
        df_f = _read_pending(con, "fisico", "FIS", ["ID", "NRBRM"])
        df_c = _read_pending(con, "contabil", "CTB", ["ID", "BEM_ANTERIOR", "INC"])
        cand_f, cand_c = len(df_f), len(df_c)

        df_f["NRBRM"] = pd.to_numeric(df_f["NRBRM"], errors="coerce")
        df_c["BEM_ANTERIOR"] = pd.to_numeric(df_c["BEM_ANTERIOR"], errors="coerce")
        df_c["INC"] = pd.to_numeric(df_c["INC"], errors="coerce").fillna(0).astype(int)

        df_f = df_f.dropna(subset=["NRBRM"])
        df_c = df_c.dropna(subset=["BEM_ANTERIOR"])
        df_c = df_c.loc[df_c["INC"] == 0].copy()

        if df_f.empty or df_c.empty:
            return MatchStats(st, cand_f, cand_c, 0)

        pairs = _pair_1to1_by_value(df_f.rename(columns={"NRBRM":"KEY"}), df_c.rename(columns={"BEM_ANTERIOR":"KEY"}), "KEY", "KEY")
        pairs["INC_CONTABIL"] = 0
        created = _bulk_insert_pairs(con, pairs, st)
        return MatchStats(st, cand_f, cand_c, int(created))
    finally:
        try: con.close()
        except Exception: pass


def run_regra_exata(db_path: str, key: str, st: str, ctb_inc0_only: bool = False) -> MatchStats:
    con = connect(db_path)
    try:
        init_db(con)
        df_f = _read_pending(con, "fisico", "FIS", ["ID", key])
        df_c = _read_pending(con, "contabil", "CTB", ["ID", key, "INC"])
        cand_f, cand_c = len(df_f), len(df_c)

        # normalize key (string) and exclude empty
        df_f[key] = df_f[key].astype(str).str.strip()
        df_c[key] = df_c[key].astype(str).str.strip()
        df_f = df_f.loc[df_f[key] != ""].copy()
        df_c = df_c.loc[df_c[key] != ""].copy()

        df_c["INC"] = pd.to_numeric(df_c["INC"], errors="coerce").fillna(0).astype(int)
        if ctb_inc0_only:
            df_c = df_c.loc[df_c["INC"] == 0].copy()

        if df_f.empty or df_c.empty:
            return MatchStats(st, cand_f, cand_c, 0)

        pairs = _pair_1to1_by_value(df_f, df_c, key, key)
        pairs["INC_CONTABIL"] = 0
        created = _bulk_insert_pairs(con, pairs, st)
        return MatchStats(st, cand_f, cand_c, int(created))
    finally:
        try: con.close()
        except Exception: pass


def run_propagacao_incorporados(db_path: str) -> MatchStats:
    st = "CA - INC"
    con = connect(db_path)
    try:
        init_db(con)
        cols_depara = _table_columns(con, "depara")
        par_col = "par_id" if "par_id" in cols_depara else "PAR_ID"
        st_col = "st_conciliacao" if "st_conciliacao" in cols_depara else "ST_CONCILIACAO"
        idf_col = "id_fisico" if "id_fisico" in cols_depara else "ID_FISICO"
        idc_col = "id_contabil" if "id_contabil" in cols_depara else "ID_CONTABIL"
        nr_col = "nrbrm" if "nrbrm" in cols_depara else "NRBRM"
        inc_col = "inc_contabil" if "inc_contabil" in cols_depara else "INC_CONTABIL"

        # pais conciliados (inc_contabil==0) e do automático
        df_pais = pd.read_sql_query(
            f"""
            SELECT COALESCE({_qident(idf_col)}, 0) AS "ID_FISICO",
                   {_qident(idc_col)} AS "ID_CONTABIL",
                   {_qident(nr_col)} AS "NRBRM"
            FROM {_qident('depara')}
            WHERE COALESCE({_qident(inc_col)}, 0) = 0
              """,
            con,
        )

        cand_f = int(df_pais["ID_FISICO"].nunique()) if not df_pais.empty else 0

        # filhos contábeis pendentes (inc != 0) pelo mesmo nrbrm
        df_filhos = _read_pending(con, "contabil", "CTB", ["ID", "NRBRM", "INC"])
        cand_c = len(df_filhos)

        if df_pais.empty or df_filhos.empty:
            return MatchStats(st, cand_f, cand_c, 0)

        df_filhos["NRBRM"] = pd.to_numeric(df_filhos["NRBRM"], errors="coerce")
        df_filhos["INC"] = pd.to_numeric(df_filhos["INC"], errors="coerce").fillna(0).astype(int)
        df_filhos = df_filhos.loc[df_filhos["INC"] != 0].copy()
        df_filhos = df_filhos.dropna(subset=["NRBRM"])

        df_pais["NRBRM"] = pd.to_numeric(df_pais["NRBRM"], errors="coerce")
        df_pais = df_pais.dropna(subset=["NRBRM"])

        if df_pais.empty or df_filhos.empty:
            return MatchStats(st, cand_f, cand_c, 0)

        m = df_filhos.merge(df_pais[["ID_FISICO", "NRBRM"]], on="NRBRM", how="inner")
        if m.empty:
            return MatchStats(st, cand_f, cand_c, 0)

        pairs = pd.DataFrame(
            {
                "ID_FISICO": m["ID_FISICO"],
                "ID_CONTABIL": m["ID"],
                "NRBRM": m["NRBRM"],
                "INC_CONTABIL": m["INC"],
            }
        )
        created = _bulk_insert_pairs(con, pairs, st)
        return MatchStats(st, cand_f, cand_c, int(created))
    finally:
        try: con.close()
        except Exception: pass
