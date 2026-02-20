# importer_v2.py
from __future__ import annotations

import sqlite3
from typing import Dict, Iterable, List, Tuple, Callable, Optional

import pandas as pd

from db_utils_v2 import connect, init_db

# Mapeamento dos cabeçalhos do Excel (modelo do cliente) -> colunas do SQLite (schema V2)
MAP_FISICO: Dict[str, str] = {
    "ID": "ID",
    "FILIAL": "FILIAL",
    "DESC.FILIAL": "DESC_FILIAL",
    "CCUSTO": "CCUSTO",
    "DESCR.CCUSTO": "DESCR_CCUSTO",
    "LOCAL": "LOCAL",
    "DESCR. LOCAL": "DESCR_LOCAL",
    "NRBRM": "NRBRM",
    "INC.": "INC",
    "DESCRICAO": "DESCRICAO",
    "MARCA": "MARCA",
    "MODELO": "MODELO",
    "SERIE": "SERIE",
    "DIMENSAO": "DIMENSAO",
    "CAPACIDADE": "CAPACIDADE",
    "TAG": "TAG",
    "BEM ANTERIOR": "BEM_ANTERIOR",
    "CONDIC": "CONDIC",
    "QTD": "QTD",
    "FRAG": "FRAG",
}

MAP_CONTABIL: Dict[str, str] = {
    "ID": "ID",
    "DESCR. CONTA": "DESC_CONTA",
    "COD. CONTA": "COD_CONTA",
    "FILIAL": "FILIAL",
    "DESC.FILIAL": "DESC_FILIAL",
    "CCUSTO": "CCUSTO",
    "DESCR.CCUSTO": "DESCR_CCUSTO",
    "LOCAL": "LOCAL",
    "DESCR. LOCAL": "DESCR_LOCAL",
    "NRBRM": "NRBRM",
    "INC.": "INC",
    "DESCRICAO": "DESCRICAO",
    "MARCA": "MARCA",
    "MODELO": "MODELO",
    "SERIE": "SERIE",
    "DIMENSAO": "DIMENSAO",
    "CAPACIDADE": "CAPACIDADE",
    "TAG": "TAG",
    "BEM ANTERIOR": "BEM_ANTERIOR",
    "QTD": "QTD",
    "DT. AQUISIÇÃO": "DT_AQUISICAO",
    "VLR. AQUISIÇÃO": "VLR_AQUISICAO",
    "DEP. ACUMULADA": "DEP_ACUMULADA",
    "VLR. RESIDUAL": "VLR_RESIDUAL",
    "FRAG": "FRAG",
}

INT_COLS_FIS = {"ID","NRBRM","INC","QTD"}
INT_COLS_CTB = {"ID","NRBRM","INC","QTD"}
REAL_COLS_CTB = {"VLR_AQUISICAO","DEP_ACUMULADA","VLR_RESIDUAL"}

TEXT_COLS_FIS = {"DESC_FILIAL","DESCR_CCUSTO","DESCR_LOCAL","DESCRICAO","MARCA","MODELO","SERIE","DIMENSAO","CAPACIDADE","TAG","BEM_ANTERIOR","CONDIC","FRAG"}
TEXT_COLS_CTB = {"DESC_FILIAL","DESCR_CCUSTO","DESCR_LOCAL","DESCRICAO","MARCA","MODELO","SERIE","DIMENSAO","CAPACIDADE","TAG","BEM_ANTERIOR","DT_AQUISICAO","FRAG","COD_CONTA","DESC_CONTA"}

def _norm_text(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str)
    s = s.str.replace(r"\.0$", "", regex=True).str.strip()
    return s

def _to_int_series(s: pd.Series) -> pd.Series:
    # converte "123.0" -> 123, e NA-safe
    s2 = pd.to_numeric(s, errors="coerce")
    return s2.astype("Int64")

def _to_real_series(s: pd.Series) -> pd.Series:
    s2 = pd.to_numeric(s, errors="coerce")
    return s2.astype(float)

def _chunked(iterable: List[Tuple], size: int) -> Iterable[List[Tuple]]:
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]

def _prepare_fisico(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=MAP_FISICO).copy()

    # garante todas colunas do schema, mesmo se vierem faltando
    for col in (INT_COLS_FIS | TEXT_COLS_FIS):
        if col not in df.columns:
            df[col] = pd.NA

    for col in INT_COLS_FIS:
        df[col] = _to_int_series(df[col])

    for col in TEXT_COLS_FIS:
        df[col] = _norm_text(df[col])

    # colunas de normalização (para regras "contém" futuras)
    df["DESC_NORM"] = _norm_text(df["DESCRICAO"]).str.upper()
    df["MARCA_NORM"] = _norm_text(df["MARCA"]).str.upper()
    df["MODELO_NORM"] = _norm_text(df["MODELO"]).str.upper()
    df["SERIE_NORM"] = _norm_text(df["SERIE"]).str.upper()
    df["TAG_NORM"] = _norm_text(df["TAG"]).str.upper()
    df["BEM_ANT_NORM"] = _norm_text(df["BEM_ANTERIOR"]).str.upper()

    return df

def _prepare_contabil(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=MAP_CONTABIL).copy()

    for col in (INT_COLS_CTB | REAL_COLS_CTB | TEXT_COLS_CTB):
        if col not in df.columns:
            df[col] = pd.NA

    for col in INT_COLS_CTB:
        df[col] = _to_int_series(df[col])

    for col in REAL_COLS_CTB:
        df[col] = _to_real_series(df[col])

    for col in TEXT_COLS_CTB:
        df[col] = _norm_text(df[col])

    df["DESC_NORM"] = _norm_text(df["DESCRICAO"]).str.upper()
    df["MARCA_NORM"] = _norm_text(df["MARCA"]).str.upper()
    df["MODELO_NORM"] = _norm_text(df["MODELO"]).str.upper()
    df["SERIE_NORM"] = _norm_text(df["SERIE"]).str.upper()
    df["TAG_NORM"] = _norm_text(df["TAG"]).str.upper()
    df["BEM_ANT_NORM"] = _norm_text(df["BEM_ANTERIOR"]).str.upper()

    return df

def _bulk_insert(
    con: sqlite3.Connection,
    table: str,
    df: pd.DataFrame,
    cols: List[str],
    *,
    chunk_size: int = 8000,
    progress_cb: Optional[Callable[[float, str], None]] = None,
    progress_range: Tuple[float, float] = (0.0, 100.0),
    label: str = "",
) -> int:
    """Insere DataFrame em chunks, com callback opcional de progresso (percentual 0-100)."""
    # transforma Int64 -> Python int/None e NaN -> None
    records: List[Tuple] = []
    for row in df[cols].itertuples(index=False, name=None):
        rec = []
        for v in row:
            if pd.isna(v):
                rec.append(None)
            else:
                try:
                    rec.append(v.item() if hasattr(v, "item") else v)
                except Exception:
                    rec.append(v)
        records.append(tuple(rec))

    placeholders = ",".join(["?"] * len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders});"
    cur = con.cursor()

    total_rows = len(records)
    done = 0
    p0, p1 = progress_range

    if progress_cb:
        progress_cb(p0, f"{label} (0/{total_rows})")

    for chunk in _chunked(records, chunk_size):
        cur.executemany(sql, chunk)
        done += len(chunk)
        if progress_cb and total_rows:
            frac = done / total_rows
            progress_cb(p0 + (p1 - p0) * frac, f"{label} ({done}/{total_rows})")

    if progress_cb:
        progress_cb(p1, f"{label} ({total_rows}/{total_rows})")

    return done

def import_to_db(fis_path: str, ctb_path: str, db_path: str, progress_cb: Optional[Callable[[float, str], None]] = None) -> str:
    """
    Importa BsFisico.xlsx e BsContabil.xlsx (arquivos separados) para SQLite.
    - Aplica mapeamento de colunas conforme modelo.
    - NA-safe (não quebra com valores vazios em colunas inteiras).
    - Usa chunking no executemany para evitar "too many SQL variables".
    """
    if progress_cb:
        progress_cb(0.0, "Lendo arquivos Excel...")
    df_f = pd.read_excel(fis_path)
    df_c = pd.read_excel(ctb_path)
    if progress_cb:
        progress_cb(5.0, "Preparando dados (normalização)...")

    df_f = _prepare_fisico(df_f)
    df_c = _prepare_contabil(df_c)
    if progress_cb:
        progress_cb(15.0, "Abrindo banco e preparando tabelas...")

    con = connect(db_path)
    try:
        init_db(con)
        if progress_cb:
            progress_cb(20.0, "Limpando tabelas...")

        con.execute("BEGIN;")
        try:
            con.execute("DELETE FROM fisico;")
            con.execute("DELETE FROM contabil;")
            con.execute("DELETE FROM conciliados;")
            con.execute("DELETE FROM depara;")

            fis_cols = [
                "ID","FILIAL","DESC_FILIAL","CCUSTO","DESCR_CCUSTO","LOCAL","DESCR_LOCAL",
                "NRBRM","INC","DESCRICAO","MARCA","MODELO","SERIE","DIMENSAO","CAPACIDADE",
                "TAG","BEM_ANTERIOR","CONDIC","QTD","FRAG",
                "DESC_NORM","MARCA_NORM","MODELO_NORM","SERIE_NORM","TAG_NORM","BEM_ANT_NORM"
            ]
            ctb_cols = [
                "ID","COD_CONTA","DESC_CONTA","FILIAL","DESC_FILIAL","CCUSTO","DESCR_CCUSTO","LOCAL","DESCR_LOCAL",
                "NRBRM","INC","DESCRICAO","MARCA","MODELO","SERIE","DIMENSAO","CAPACIDADE",
                "TAG","BEM_ANTERIOR","QTD","DT_AQUISICAO","VLR_AQUISICAO","DEP_ACUMULADA","VLR_RESIDUAL","FRAG",
                "DESC_NORM","MARCA_NORM","MODELO_NORM","SERIE_NORM","TAG_NORM","BEM_ANT_NORM"
            ]

            n_f = _bulk_insert(con, "fisico", df_f, fis_cols, progress_cb=progress_cb, progress_range=(20.0, 55.0), label="Inserindo BsFisico")
            n_c = _bulk_insert(con, "contabil", df_c, ctb_cols, progress_cb=progress_cb, progress_range=(55.0, 95.0), label="Inserindo BsContabil")

            if progress_cb:
                progress_cb(98.0, "Finalizando importação...")
            con.commit()
            if progress_cb:
                progress_cb(100.0, "Importação concluída.")
        except Exception:
            con.rollback()
            raise

        return f"Importação concluída. Físico: {n_f} linhas | Contábil: {n_c} linhas."
    finally:
        con.close()

def import_bases(fis_path: str, ctb_path: str, db_path: str, reset: bool = True, progress_cb: Optional[Callable[[float, str], None]] = None) -> dict:
    """
    Wrapper usado pela interface.
    - Se reset=True, o import_to_db já limpa as tabelas (fisico/contabil/conciliados/depara) antes de inserir.
    Retorna contagens (linhas importadas).
    """
    msg = import_to_db(fis_path, ctb_path, db_path, progress_cb=progress_cb)
    # Extrai números do texto da mensagem (fallback seguro)
    counts = {"msg": msg}
    try:
        m = re.search(r"Físico:\s*(\d+)\s*linhas\s*\|\s*Contábil:\s*(\d+)\s*linhas", msg)
        if m:
            counts["fis_total"] = int(m.group(1))
            counts["ctb_total"] = int(m.group(2))
    except Exception:
        pass
    return counts
