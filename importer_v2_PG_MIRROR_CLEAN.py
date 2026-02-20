"""
EVS - Importador com espelhamento em PostgreSQL (schema limpo)

- Se PostgreSQL estiver disponível, espelha dados mínimos (id, nrbrm, inc, frag)
  para tabelas: fisico, contabil, depara, conciliados, meta.
- Mantém o SQLite como fonte completa (sem alterar sua lógica atual).

Config PG (padrão):
  host=localhost port=5432 db=evs_conciliador user=evs password=evs123
Variáveis:
  EVS_PG_HOST, EVS_PG_PORT, EVS_PG_DB, EVS_PG_USER, EVS_PG_PASSWORD
"""

from __future__ import annotations

import os
from typing import Optional, Tuple

import pandas as pd

try:
    import psycopg2
except Exception:  # pragma: no cover
    psycopg2 = None


PG_SCHEMA_SQL = """
BEGIN;

DROP TABLE IF EXISTS conciliados;
DROP TABLE IF EXISTS depara;
DROP TABLE IF EXISTS fisico;
DROP TABLE IF EXISTS contabil;
DROP TABLE IF EXISTS meta;
DROP SEQUENCE IF EXISTS depara_par_id_seq;

CREATE TABLE meta (
  k TEXT PRIMARY KEY,
  v TEXT
);

CREATE TABLE fisico (
  id BIGINT PRIMARY KEY,
  nrbrm TEXT,
  inc INTEGER,
  frag TEXT
);

CREATE TABLE contabil (
  id BIGINT PRIMARY KEY,
  nrbrm TEXT,
  inc INTEGER,
  frag TEXT
);

CREATE TABLE depara (
  par_id BIGINT PRIMARY KEY,
  st_conciliacao TEXT,
  id_fisico BIGINT,
  id_contabil BIGINT,
  nrbrm TEXT,
  inc_contabil INTEGER,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE SEQUENCE depara_par_id_seq;
ALTER TABLE depara ALTER COLUMN par_id SET DEFAULT nextval('depara_par_id_seq');

CREATE TABLE conciliados (
  base TEXT NOT NULL,
  base_id BIGINT NOT NULL,
  par_id BIGINT NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (base, base_id)
);

CREATE INDEX fisico_nrbrm_inc_idx   ON fisico (nrbrm, inc);
CREATE INDEX contabil_nrbrm_inc_idx ON contabil (nrbrm, inc);
CREATE INDEX fisico_frag_idx        ON fisico (frag);
CREATE INDEX contabil_frag_idx      ON contabil (frag);
CREATE INDEX conciliados_parid_idx  ON conciliados (par_id);

COMMIT;
"""


def _pg_dsn() -> str:
    host = os.getenv("EVS_PG_HOST", "localhost")
    port = os.getenv("EVS_PG_PORT", "5432")
    db = os.getenv("EVS_PG_DB", "evs_conciliador")
    user = os.getenv("EVS_PG_USER", "evs")
    pwd = os.getenv("EVS_PG_PASSWORD", "evs123")
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"


def pg_available() -> bool:
    if psycopg2 is None:
        return False
    try:
        conn = psycopg2.connect(_pg_dsn())
        conn.close()
        return True
    except Exception:
        return False


def pg_reset_schema() -> None:
    """Recria o schema limpo (A) no PostgreSQL."""
    if psycopg2 is None:
        raise RuntimeError("psycopg2 não está instalado")
    conn = psycopg2.connect(_pg_dsn())
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(PG_SCHEMA_SQL)
    cur.close()
    conn.close()


def _norm_int(v) -> Optional[int]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s == "" or s.lower() == "nan":
        return None
    try:
        return int(s)
    except Exception:
        return None


def _norm_text(v) -> Optional[str]:
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s.lower() == "nan" or s == "":
        return None
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def _load_excel_minimal(path_fisico: str, path_contabil: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_f = pd.read_excel(path_fisico, engine="openpyxl")
    df_c = pd.read_excel(path_contabil, engine="openpyxl")

    def pick(df: pd.DataFrame, name: str) -> str:
        for c in df.columns:
            if str(c).strip().lower() == name.lower():
                return c
        raise KeyError(f"Coluna obrigatória não encontrada: {name}")

    # Físico
    idc = pick(df_f, "ID")
    nr = pick(df_f, "NrBrm")
    inc = pick(df_f, "Inc.")
    frag = pick(df_f, "FRAG")

    df_f2 = pd.DataFrame({
        "id": df_f[idc].map(_norm_int),
        "nrbrm": df_f[nr].map(_norm_text),
        "inc": df_f[inc].map(_norm_int),
        "frag": df_f[frag].map(_norm_text),
    }).dropna(subset=["id"])

    # Contábil
    idc = pick(df_c, "ID")
    nr = pick(df_c, "NrBrm")
    inc = pick(df_c, "Inc.")
    frag = pick(df_c, "FRAG")

    df_c2 = pd.DataFrame({
        "id": df_c[idc].map(_norm_int),
        "nrbrm": df_c[nr].map(_norm_text),
        "inc": df_c[inc].map(_norm_int),
        "frag": df_c[frag].map(_norm_text),
    }).dropna(subset=["id"])

    return df_f2, df_c2


def pg_import_from_excel(path_fisico: str, path_contabil: str) -> Tuple[int, int]:
    """Zera e importa no PostgreSQL (schema limpo). Retorna (qtd_fisico, qtd_contabil)."""
    if psycopg2 is None:
        raise RuntimeError("psycopg2 não está instalado")
    df_f, df_c = _load_excel_minimal(path_fisico, path_contabil)

    conn = psycopg2.connect(_pg_dsn())
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE conciliados, depara, fisico, contabil, meta RESTART IDENTITY;")

    rows_f = [(int(r.id), r.nrbrm, int(r.inc) if r.inc is not None else None, r.frag)
              for r in df_f.itertuples(index=False)]
    cur.executemany("INSERT INTO fisico (id, nrbrm, inc, frag) VALUES (%s,%s,%s,%s);", rows_f)

    rows_c = [(int(r.id), r.nrbrm, int(r.inc) if r.inc is not None else None, r.frag)
              for r in df_c.itertuples(index=False)]
    cur.executemany("INSERT INTO contabil (id, nrbrm, inc, frag) VALUES (%s,%s,%s,%s);", rows_c)

    conn.commit()
    cur.close()
    conn.close()
    return len(rows_f), len(rows_c)
