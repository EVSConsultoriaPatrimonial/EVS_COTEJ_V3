# manual_db_v2.py
from __future__ import annotations

import sqlite3
import re
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any

import pandas as pd
from db_utils_v2 import connect as connect_auto


@dataclass(frozen=True)
class PendingCounts:
    fis: int
    ctb: int


def connect(db_path: str) -> sqlite3.Connection:
    con = connect_auto(db_path)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")
        con.execute("PRAGMA temp_store=MEMORY;")
    except Exception:
        pass
    return con



# ---------------- Helpers: colunas e listas para filtros ----------------

def _list_columns(con: sqlite3.Connection, table: str) -> list[str]:
    cur = con.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]

def _resolve_column(con: sqlite3.Connection, table: str, candidates: list[str], *, contains: str | None = None) -> str | None:
    cols = _list_columns(con, table)
    cols_upper = {c.upper(): c for c in cols}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    if contains:
        target = contains.upper()
        for c in cols:
            if target in c.upper():
                return c
    return None

def get_distinct_values(con: sqlite3.Connection, field: str, *, include_fis: bool = True, include_ctb: bool = True, limit: int = 5000) -> list[str]:
    field = (field or "").strip().upper()

    def _q_distinct(table: str, col: str) -> str:
        return f"SELECT DISTINCT TRIM(CAST({col} AS TEXT)) AS v FROM {table} WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS TEXT)) <> ''"

    parts: list[str] = []

    if field in ("FILIAL", "CCUSTO", "LOCAL"):
        if include_fis:
            col = _resolve_column(con, "fisico", [field], contains=field)
            if (not col) and field == "CCUSTO":
                col = _resolve_column(con, "fisico", ["CCUSTO","CCUST","CENTRO_CUSTO","CENTRO DE CUSTO","CENTRODECUSTO","CENTRO_DE_CUSTO"], contains="CUSTO")
            if col:
                parts.append(_q_distinct("fisico", col))
        if include_ctb:
            col = _resolve_column(con, "contabil", [field], contains=field)
            if (not col) and field == "CCUSTO":
                col = _resolve_column(con, "contabil", ["CCUSTO","CCUST","CENTRO_CUSTO","CENTRO DE CUSTO","CENTRODECUSTO","CENTRO_DE_CUSTO"], contains="CUSTO")
            if col:
                parts.append(_q_distinct("contabil", col))

    elif field == "CONDIC":
        col = _resolve_column(con, "fisico", ["CONDIC", "CONDICAO_USO", "CONDICAO", "COND"], contains="CONDIC")
        if col:
            parts.append(_q_distinct("fisico", col))

    elif field == "ANO_CTB":
        col = _resolve_column(con, "contabil", ["DT_AQUISICAO", "DT.AQUISIÇÃO", "DT AQUISICAO", "DT_AQUIS"], contains="AQUIS")
        if col:
            parts.append(f"SELECT DISTINCT SUBSTR(COALESCE({col},''),1,4) AS v FROM contabil WHERE {col} IS NOT NULL AND TRIM(COALESCE({col},'')) <> ''")

    if not parts:
        return []

    sql = " UNION ".join(parts) + " ORDER BY v LIMIT ?;"
    cur = con.execute(sql, (int(limit),))
    out: list[str] = []
    for (v,) in cur.fetchall():
        v = (v or "").strip()
        if v:
            out.append(v)
    return out

def get_counts(con: sqlite3.Connection) -> PendingCounts:
    q_f = """
    SELECT COUNT(1)
    FROM fisico f
    WHERE f.ID IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='FIS' AND c.ID=f.ID);
    """
    q_c = """
    SELECT COUNT(1)
    FROM contabil t
    WHERE t.ID IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='CTB' AND c.ID=t.ID);
    """
    fis = int(con.execute(q_f).fetchone()[0])
    ctb = int(con.execute(q_c).fetchone()[0])
    return PendingCounts(fis=fis, ctb=ctb)


def _norm_like(value: str) -> str:
    return " ".join((value or "").upper().split())


def _apply_like(field_sql: str, value: str) -> Tuple[str, list]:
    value = (value or "").strip()
    if not value:
        return "", []
    norm = _norm_like(value)
    return f" AND {field_sql} LIKE ? ", [f"%{norm}%"]


def _apply_eq_int(field_sql: str, value: str) -> Tuple[str, list]:
    value = (value or "").strip()
    if not value:
        return "", []
    try:
        v = int(float(value))
    except Exception:
        return f" AND {field_sql} = ? ", [-999999999]
    return f" AND {field_sql} = ? ", [v]

def _apply_eq_num_or_text(field_sql: str, value: str) -> Tuple[str, list]:
    """Comparação exata flexível:
    - Se o valor for numérico, compara como inteiro (tolerando '123.0')
    - Caso contrário, compara como texto (case-insensitive), tolerando espaços.
    Isso resolve casos como CCUSTO = 'TZ14700'.
    """
    value = (value or "").strip()
    if not value:
        return "", []
    # tenta numérico
    try:
        v = int(float(value))
        return f" AND {field_sql} = ? ", [v]
    except Exception:
        norm = _norm_like(value)
        return f" AND UPPER(COALESCE({field_sql},'')) = ? ", [norm]





def _eq_text(field_sql: str, value: str) -> Tuple[str, list]:
    """Comparação exata de texto (case-insensitive), tolerante a espaços."""
    value = (value or "").strip()
    if not value:
        return "", []
    norm = _norm_like(value)
    return f" AND UPPER(COALESCE({field_sql},'')) = ? ", [norm]


def _apply_desc_terms(field_sql: str, desc1: str, desc2: str, desc3: str, mode: str) -> Tuple[str, list]:
    terms = [t.strip() for t in [desc1, desc2, desc3] if (t or "").strip()]
    if not terms:
        return "", []
    mode = (mode or "E").strip().upper()
    if mode not in ("E", "OU"):
        mode = "E"

    clauses = []
    params: list = []
    for t in terms:
        n = _norm_like(t)
        clauses.append(f"{field_sql} LIKE ?")
        params.append(f"%{n}%")

    joiner = " AND " if mode == "E" else " OR "
    return f" AND ({joiner.join(clauses)}) ", params


def load_pending_manual(
    con: sqlite3.Connection,
    base: str,
    *,
    limit: int = 500,
    desc1: str = "",
    desc2: str = "",
    desc3: str = "",
    desc_mode: str = "E",
    filial: str = "",
    ccusto: str = "",
    local: str = "",
    condic: str = "",
    data_ctb_ano: str = "",
    only_inc0: bool = False,
) -> pd.DataFrame:
    """
    Pendentes (Manual): filtros do layout novo.
    - descrição: até 3 termos, com modo E/OU (aplicado ao campo *_NORM)
    - filial/ccusto/local: comparação numérica exata (se informado)
    - data_ctb_ano: aplica SOMENTE para base CTB, comparando ano de DT_AQUISICAO (YYYY)
    """
    if base not in ("FIS", "CTB"):
        raise ValueError("base inválida")

    table = "fisico" if base == "FIS" else "contabil"
    where = """
    WHERE t.ID IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE=? AND c.ID=t.ID)
    """
    params: list = [base]

    extra, p = _apply_desc_terms("t.DESC_NORM", desc1, desc2, desc3, desc_mode)
    where += extra; params += p

    extra, p = _apply_eq_int("t.FILIAL", filial); where += extra; params += p
    extra, p = _apply_eq_num_or_text("t.CCUSTO", ccusto); where += extra; params += p
    extra, p = _apply_eq_int("t.LOCAL", local); where += extra; params += p

    # Condição de uso existe apenas na base física (campo CONDIC)
    if base == "FIS" and (condic or "").strip():
        col_cond = _resolve_column(
            con,
            "fisico",
            ["CONDIC", "CONDICAO_USO", "CONDICAO", "COND"],
            contains="CONDIC",
        )
        if col_cond:
            extra, p = _eq_text(f"t.{col_cond}", condic)
            where += extra
            params += p

    if base == "CTB" and only_inc0:
        where += " AND COALESCE(t.INC,0) = 0 "

    if base == "CTB" and (data_ctb_ano or "").strip():
        # DT_AQUISICAO esperado como texto ISO ou algo parseável; usamos substr(,1,4)
        ano = (data_ctb_ano or "").strip()
        if len(ano) >= 4:
            ano = ano[:4]
        where += " AND SUBSTR(COALESCE(t.DT_AQUISICAO,''),1,4) = ? "
        params.append(ano)

    q = f"SELECT t.* FROM {table} t {where} ORDER BY COALESCE(t.DESCRICAO,''), t.ID LIMIT ?;"
    params.append(int(limit))
    return pd.read_sql_query(q, con, params=params)


# -------- Automático (02): retorna SOMENTE candidatos conforme regra --------

AUTO02_RULES = {
    "1": "Série física contida na descrição contábil (>=4)",
    "2": "Série contábil contida na descrição física (>=4)",
    "3": "Modelo = Modelo (exato)",
    "4": "Modelo físico contido na descrição contábil (>=4)",
    "5": "Tag física contida na descrição contábil (>=4)",
    "6": "Descrição física semelhante à descrição contábil (>=2 atributos)",

}



# --- Regra 6 (Auto02): Similaridade por descrição (>=2 atributos) ---
_STOP_LABELS = {"MCA", "MOD", "SERIE", "CAP", "CAPACIDADE", "TAG"}

def _strip_noise_fields(desc: str) -> str:
    """Remove segmentos do tipo 'MCA: ...', 'MOD: ...', 'SERIE: ...', 'CAP: ...', 'CAPACIDADE: ...', 'TAG: ...'.
    A ideia é evitar que esses trechos dominem a similaridade.
    """
    if not desc:
        return ""
    s = str(desc)
    # normaliza espaços
    s = s.replace("\n", " ").replace("\r", " ")
    # remove blocos rotulados (não-guloso até próximo rótulo ou fim)
    # Ex.: "MCA: LG MOD: 32LW300C SERIE: 123" => remove tudo que está após cada rótulo
    pattern = r"\b(MCA|MOD|SERIE|CAP|CAPACIDADE|TAG)\s*[:=]\s*.*?(?=\b(MCA|MOD|SERIE|CAP|CAPACIDADE|TAG)\b\s*[:=]|$)"
    try:
        s = re.sub(pattern, " ", s, flags=re.IGNORECASE)
    except Exception:
        # se regex falhar por algum motivo, segue com o texto original
        pass
    return s

def _desc_attr_set(desc: str) -> set:
    """Extrai um conjunto de 'atributos' da descrição para comparar semelhança.
    - Remove blocos MCA/MOD/SERIE/CAP/CAPACIDADE/TAG
    - Usa tokens com len>=3 ou numéricos com len>=2
    - Usa prefixo de 3 caracteres para tokens alfabéticos (ex.: 'cadeira' -> 'cad')
    """
    if not desc:
        return set()
    s = _strip_noise_fields(desc).upper()
    # mantém letras/números como tokens
    tokens = re.findall(r"[A-Z0-9]+", s)
    out = set()
    for tok in tokens:
        if tok in _STOP_LABELS:
            continue
        if tok.isdigit():
            if len(tok) >= 2:
                out.add(tok)
            continue
        if len(tok) >= 3:
            out.add(tok[:3])  # atributo por prefixo (>=3)
    return out
def _auto02_base_filters(alias: str, *, desc1: str, desc2: str, desc3: str, desc_mode: str,
                         filial: str, ccusto: str, local: str, condic: str) -> Tuple[str, list]:
    where = ""
    params: list = []

    extra, p = _apply_desc_terms(f"{alias}.DESC_NORM", desc1, desc2, desc3, desc_mode)
    where += extra; params += p
    extra, p = _apply_eq_int(f"{alias}.FILIAL", filial); where += extra; params += p
    extra, p = _apply_eq_num_or_text(f"{alias}.CCUSTO", ccusto); where += extra; params += p
    extra, p = _apply_eq_int(f"{alias}.LOCAL", local); where += extra; params += p

    # Condição de uso existe apenas na base física (físico)
    if alias == "f" and (condic or "").strip():
        extra, p = _eq_text(f"{alias}.CONDIC", condic)
        where += extra; params += p

    return where, params


def load_candidates_auto02(
    con: sqlite3.Connection,
    rule_id: str,
    *,
    limit_each: int = 500,
    desc1: str = "",
    desc2: str = "",
    desc3: str = "",
    desc_mode: str = "E",
    filial: str = "",
    ccusto: str = "",
    local: str = "",
    condic: str = "",
    data_ctb_ano: str = "",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Automático (02) — Assistido:
    Retorna (df_fis, df_ctb) contendo SOMENTE itens pendentes que participam de pelo menos
    um possível match segundo a regra selecionada.
    Observações:
    - Não grava nada.
    - Respeita pendência (não existe em conciliados).
    - Regras "contém" usam mínimo 4 caracteres no termo gerador.
    - data_ctb_ano filtra somente contábil (ano DT_AQUISICAO).
    """
    rid = (rule_id or "").strip()
    if rid not in AUTO02_RULES:
        return pd.DataFrame(), pd.DataFrame()

    # filtros comuns
    w_fis, p_fis = _auto02_base_filters("f", desc1=desc1, desc2=desc2, desc3=desc3, desc_mode=desc_mode,
                                       filial=filial, ccusto=ccusto, local=local, condic=condic)
    w_ctb, p_ctb = _auto02_base_filters("t", desc1=desc1, desc2=desc2, desc3=desc3, desc_mode=desc_mode,
                                       filial=filial, ccusto=ccusto, local=local, condic="")

    # pendentes
    pend_fis = "NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='FIS' AND c.ID=f.ID)"
    pend_ctb = "NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='CTB' AND c.ID=t.ID)"

    # data CTB (ano)
    if (data_ctb_ano or "").strip():
        ano = (data_ctb_ano or "").strip()
        if len(ano) >= 4:
            ano = ano[:4]
        w_ctb += " AND SUBSTR(COALESCE(t.DT_AQUISICAO,''),1,4) = ? "
        p_ctb.append(ano)

    
    # Regra 6: similaridade por descrição (>=2 atributos)
    if rid == "6":
        # carrega pendentes (com filtros) e identifica participantes via índice invertido por atributos
        qf = f"""SELECT * FROM fisico f
                  WHERE f.ID IS NOT NULL AND {pend_fis} {w_fis}
                  ORDER BY COALESCE(f.DESCRICAO,''), f.ID LIMIT ?;"""
        qc = f"""SELECT * FROM contabil t
                  WHERE t.ID IS NOT NULL AND {pend_ctb} AND COALESCE(t.INC,0)=0 {w_ctb}
                  ORDER BY COALESCE(t.DESCRICAO,''), t.ID LIMIT ?;"""
        df_f = pd.read_sql_query(qf, con, params=p_fis + [int(limit_each)])
        df_c = pd.read_sql_query(qc, con, params=p_ctb + [int(limit_each)])

        if df_f.empty or df_c.empty:
            return pd.DataFrame(), pd.DataFrame()

        # índice invertido: atributo -> lista de IDs contábeis
        inv: Dict[str, List[int]] = {}
        c_attrs: Dict[int, set] = {}
        for _, row in df_c.iterrows():
            cid = int(row.get("ID"))
            attrs = _desc_attr_set(row.get("DESCRICAO") or row.get("DESC") or row.get("DESC_NORM") or row.get("DESC_ORIG") or "")
            c_attrs[cid] = attrs
            for a in attrs:
                inv.setdefault(a, []).append(cid)

        fis_keep = set()
        ctb_keep = set()
        # varre físicos e marca contábeis com pelo menos 2 atributos em comum
        for _, row in df_f.iterrows():
            fid = int(row.get("ID"))
            fattrs = _desc_attr_set(row.get("DESCRICAO") or row.get("DESC") or row.get("DESC_NORM") or row.get("DESC_ORIG") or "")
            if not fattrs:
                continue
            counts: Dict[int, int] = {}
            for a in fattrs:
                for cid in inv.get(a, []):
                    counts[cid] = counts.get(cid, 0) + 1
            # participantes
            hits = [cid for cid, ct in counts.items() if ct >= 2]
            if hits:
                fis_keep.add(fid)
                ctb_keep.update(hits)

        if not fis_keep or not ctb_keep:
            return pd.DataFrame(), pd.DataFrame()

        df_f = df_f[df_f["ID"].isin(sorted(fis_keep))].reset_index(drop=True)
        df_c = df_c[df_c["ID"].isin(sorted(ctb_keep))].reset_index(drop=True)
        return df_f, df_c

# Cada regra monta um JOIN e retorna DISTINCT IDs participantes
    if rid == "1":
        # série física (>=4) contida na descrição contábil
        join_cond = "LENGTH(f.SERIE_NORM) >= 4 AND t.DESC_NORM LIKE ('%' || f.SERIE_NORM || '%')"
    elif rid == "2":
        # série contábil (>=4) contida na descrição física
        join_cond = "LENGTH(t.SERIE_NORM) >= 4 AND f.DESC_NORM LIKE ('%' || t.SERIE_NORM || '%')"
    elif rid == "3":
        join_cond = "t.MODELO_NORM <> '' AND f.MODELO_NORM = t.MODELO_NORM"
    elif rid == "4":
        join_cond = "LENGTH(f.MODELO_NORM) >= 4 AND t.DESC_NORM LIKE ('%' || f.MODELO_NORM || '%')"
    else:  # rid == "5"
        join_cond = "LENGTH(f.TAG_NORM) >= 4 AND t.DESC_NORM LIKE ('%' || f.TAG_NORM || '%')"

    # Base de join: só pendentes + filtros + condição
    base_sql = f"""
    fisico f
    JOIN contabil t ON ({join_cond})
    WHERE f.ID IS NOT NULL AND t.ID IS NOT NULL
      AND {pend_fis}
      AND {pend_ctb}
      AND COALESCE(t.INC,0) = 0
      {w_fis}
      {w_ctb}
    """

    # IDs físicos
    q_f = f"SELECT DISTINCT f.ID FROM {base_sql} ORDER BY COALESCE(f.DESCRICAO,''), f.ID LIMIT ?;"
    params_f = ["dummy"]  # placeholder, we'll not use; easier to reuse p lists below

    # Para garantir ordem e params corretos, montamos params em ordem de uso:
    # pendências não usam params. w_fis params primeiro (p_fis), depois w_ctb params (p_ctb).
    params_join = p_fis + p_ctb

    ids_f = pd.read_sql_query(q_f, con, params=params_join + [int(limit_each)])
    if ids_f.empty:
        return pd.DataFrame(), pd.DataFrame()

    # IDs contábeis
    q_c = f"SELECT DISTINCT t.ID FROM {base_sql} ORDER BY COALESCE(t.DESCRICAO,''), t.ID LIMIT ?;"
    ids_c = pd.read_sql_query(q_c, con, params=params_join + [int(limit_each)])

    # Carrega linhas completas (para mostrar na UI)
    df_f = pd.read_sql_query(
        f"SELECT * FROM fisico WHERE ID IN ({','.join(['?']*len(ids_f))}) ORDER BY COALESCE(DESCRICAO,''), ID LIMIT ?;",
        con,
        params=[int(x) for x in ids_f["ID"].tolist()] + [int(limit_each)],
    )
    df_c = pd.read_sql_query(
        f"SELECT * FROM contabil WHERE ID IN ({','.join(['?']*len(ids_c))}) ORDER BY COALESCE(DESCRICAO,''), ID LIMIT ?;",
        con,
        params=[int(x) for x in ids_c["ID"].tolist()] + [int(limit_each)],
    )
    return df_f, df_c



def save_pairs(con: sqlite3.Connection, pairs: List[Tuple[int, int]], st_conciliacao: str = "MANUAL") -> int:
    """
    Grava vários pares em uma transação.

    Suporta "Não Chapeáveis" quando st_conciliacao == "DIRETA":
      - (fis_id=0, ctb_id>0) => sem físico
      - (fis_id>0, ctb_id=0) => sem contábil
      - (0,0) é ignorado

    Regras:
      - Insere em depara (PAR_ID, ST_CONCILIACAO, ID_FISICO, ID_CONTABIL, NRBRM, INC_CONTABIL)
      - Marca conciliados (bloqueio) em 'conciliados' somente para IDs > 0
    """
    if not pairs:
        return 0

    cur = con.cursor()
    saved = 0

    try:
        cur.execute("BEGIN;")

        # próximo PAR_ID
        row = cur.execute("SELECT COALESCE(MAX(PAR_ID),0) FROM depara;").fetchone()
        next_par_id = int(row[0] or 0) + 1

        depara_rows: List[Tuple[int, str, int, int, int, Optional[int]]] = []
        conc_rows: List[Tuple[str, int, int]] = []

        # Controle local para evitar duplicação dentro do mesmo SAVE
        pending_ctb_ids: set[int] = set()
        pending_fis_ids: set[int] = set()

        for fis_id_in, ctb_id_in in pairs:
            fis_id = int(fis_id_in or 0)
            ctb_id = int(ctb_id_in or 0)

            # ignora par vazio
            if fis_id <= 0 and ctb_id <= 0:
                continue

            # bloqueio: não repetir conciliados
            if fis_id > 0:
                r = cur.execute(
                    "SELECT 1 FROM conciliados WHERE BASE='FIS' AND ID=? LIMIT 1;",
                    (fis_id,),
                ).fetchone()
                if r:
                    continue

            if ctb_id > 0:
                r = cur.execute(
                    "SELECT 1 FROM conciliados WHERE BASE='CTB' AND ID=? LIMIT 1;",
                    (ctb_id,),
                ).fetchone()
                if r:
                    continue

            # busca NRBRM/INC do CTB quando existir; senão pega NRBRM do FIS
            nrbrm = 0
            inc_ctb: Optional[int] = None

            if ctb_id > 0:
                rowc = cur.execute(
                    "SELECT COALESCE(NRBRM,0), COALESCE(INC,0) FROM contabil WHERE ID=?;",
                    (ctb_id,),
                ).fetchone()
                if not rowc:
                    # CTB não existe mais / inválido
                    continue
                nrbrm = int(rowc[0] or 0)
                inc_ctb = int(rowc[1] or 0)

            elif fis_id > 0:
                rowf = cur.execute(
                    "SELECT COALESCE(NRBRM,0) FROM fisico WHERE ID=?;",
                    (fis_id,),
                ).fetchone()
                if not rowf:
                    continue
                nrbrm = int(rowf[0] or 0)
                inc_ctb = None  # sem contábil

            # bloqueia duplicação: se qualquer lado já estiver conciliado, ignora a linha
            if fis_id > 0:
                exists = cur.execute(
                    "SELECT 1 FROM conciliados WHERE BASE='FIS' AND ID=? LIMIT 1;",
                    (fis_id,),
                ).fetchone()
                if exists:
                    continue
            if ctb_id > 0:
                exists = cur.execute(
                    "SELECT 1 FROM conciliados WHERE BASE='CTB' AND ID=? LIMIT 1;",
                    (ctb_id,),
                ).fetchone()
                if exists:
                    continue

            # grava depara (IDs sempre inteiros; 0 quando ausente)
            depara_rows.append((next_par_id, st_conciliacao, fis_id, ctb_id, nrbrm, inc_ctb))

            # marca conciliados apenas quando existir ID
            if fis_id > 0:
                conc_rows.append(("FIS", fis_id, next_par_id))
                pending_fis_ids.add(fis_id)
            if ctb_id > 0:
                conc_rows.append(("CTB", ctb_id, next_par_id))
                pending_ctb_ids.add(ctb_id)

            saved += 1
            next_par_id += 1

        if depara_rows:
            cur.executemany(
                "INSERT INTO depara (PAR_ID, ST_CONCILIACAO, ID_FISICO, ID_CONTABIL, NRBRM, INC_CONTABIL) VALUES (?,?,?,?,?,?);",
                depara_rows,
            )

        if conc_rows:
            cur.executemany(
                "INSERT OR IGNORE INTO conciliados (BASE, ID, PAR_ID) VALUES (?,?,?);",
                conc_rows,
            )

        cur.execute("COMMIT;")
        return saved

    except Exception:
        cur.execute("ROLLBACK;")
        raise




def _child_status_for_origin(origin: str) -> str:
    origin_norm = (origin or "").strip().upper()
    # Mantém acentos/forma conforme usado no BD (ST_CONCILIACAO)
    if origin_norm == "MANUAL":
        return "CM - INC"
    if origin_norm == "DIRETA":
        return "CD - INC"
    if origin_norm in ("NÃO CHAPEÁVEL", "NAO CHAPEAVEL"):
        return "CN - INC"
    # fallback: se não reconhecer, mantém o próprio origin
    return origin


def save_pairs_with_family(con: sqlite3.Connection, pairs: List[Tuple[int, int]], st_conciliacao: str) -> int:
    """Grava pares e garante família contábil (pai INC=0 e filhos INC!=0) para cada NRBRM envolvido.

    - Para o PAR principal (selecionado), grava com st_conciliacao informado.
    - Para FILHOS (INC!=0) adicionados automaticamente:
        MANUAL -> "CM - INC"
        DIRETA -> "CD - INC"
        NÃO CHAPEÁVEL -> "CN - INC"

    Observações:
    - Mantém o mesmo ID_FISICO do par principal (ou 0 no não-chapeável).
    - Não duplica conciliações: respeita tabela 'conciliados'.
    """
    if not pairs:
        return 0

    cur = con.cursor()
    pending_ctb_ids: set[int] = set()  # valida/propaga filhos sem duplicar CTB no mesmo save
    pending_fis_ids: set[int] = set()  # mantém consistência do lado físico no mesmo save
    saved = 0
    child_status = _child_status_for_origin(st_conciliacao)

    try:
        cur.execute("BEGIN;")

        row = cur.execute("SELECT COALESCE(MAX(PAR_ID),0) FROM depara;").fetchone()
        next_par_id = int(row[0] or 0) + 1

        depara_rows: List[Tuple[int, str, int, int, int, Optional[int]]] = []
        conc_rows: List[Tuple[str, int, int]] = []

        # Guardar NRBRM envolvidos com o fis_id "âncora" (pode ser 0)
        involved: List[Tuple[int, int]] = []  # (nrbrm, fis_id_anchor)

        for fis_id_in, ctb_id_in in pairs:
            fis_id = int(fis_id_in or 0)
            ctb_id = int(ctb_id_in or 0)

            if fis_id <= 0 and ctb_id <= 0:
                continue

            # não repetir conciliados (bloqueio)
            if fis_id > 0:
                if cur.execute("SELECT 1 FROM conciliados WHERE BASE='FIS' AND ID=? LIMIT 1;", (fis_id,)).fetchone():
                    continue
            if ctb_id > 0:
                if (ctb_id in pending_ctb_ids) or cur.execute("SELECT 1 FROM conciliados WHERE BASE='CTB' AND ID=? LIMIT 1;", (ctb_id,)).fetchone():
                    continue

            nrbrm = 0
            inc_ctb: Optional[int] = None

            if ctb_id > 0:
                rowc = cur.execute(
                    "SELECT COALESCE(NRBRM,0), COALESCE(INC,0) FROM contabil WHERE ID=?;",
                    (ctb_id,),
                ).fetchone()
                if not rowc:
                    continue
                nrbrm = int(rowc[0] or 0)
                inc_ctb = int(rowc[1] or 0)
                involved.append((nrbrm, fis_id))
            elif fis_id > 0:
                rowf = cur.execute(
                    "SELECT COALESCE(NRBRM,0) FROM fisico WHERE ID=?;",
                    (fis_id,),
                ).fetchone()
                if not rowf:
                    continue
                nrbrm = int(rowf[0] or 0)
                inc_ctb = None

            # Se o CTB informado já for filho (INC!=0), grava com status de filho.
            st_for_row = st_conciliacao
            if ctb_id > 0 and int(inc_ctb or 0) != 0:
                st_for_row = child_status

            depara_rows.append((next_par_id, st_for_row, fis_id, ctb_id, nrbrm, inc_ctb))
            if fis_id > 0:
                conc_rows.append(("FIS", fis_id, next_par_id))
                pending_fis_ids.add(fis_id)
            if ctb_id > 0:
                conc_rows.append(("CTB", ctb_id, next_par_id))
                pending_ctb_ids.add(ctb_id)

            saved += 1
            next_par_id += 1

        # Propagação: para cada NRBRM envolvido via CTB, garantir pai + filhos.
        for nrbrm, fis_anchor in involved:
            if int(nrbrm or 0) <= 0:
                continue

            # Todos os CTB desse NRBRM (inclui pai e filhos)
            fam = cur.execute(
                "SELECT ID, COALESCE(INC,0) FROM contabil WHERE COALESCE(NRBRM,0)=? ORDER BY COALESCE(INC,0), ID;",
                (int(nrbrm),),
            ).fetchall()

            for ctb_id, inc_val in fam:
                ctb_id = int(ctb_id)
                inc_val = int(inc_val or 0)

                # já conciliado? pula
                if (ctb_id in pending_ctb_ids) or cur.execute("SELECT 1 FROM conciliados WHERE BASE='CTB' AND ID=? LIMIT 1;", (ctb_id,)).fetchone():
                    continue

                st_for_row = child_status if inc_val != 0 else st_conciliacao

                depara_rows.append((next_par_id, st_for_row, int(fis_anchor or 0), ctb_id, int(nrbrm), inc_val))
                # FIS: mesmo anchor; OR IGNORE evita duplicar
                if int(fis_anchor or 0) > 0:
                    conc_rows.append(("FIS", int(fis_anchor), next_par_id))
                    pending_fis_ids.add(int(fis_anchor))
                conc_rows.append(("CTB", ctb_id, next_par_id))
                pending_ctb_ids.add(ctb_id)

                saved += 1
                next_par_id += 1

        if depara_rows:
            cur.executemany(
                "INSERT INTO depara (PAR_ID, ST_CONCILIACAO, ID_FISICO, ID_CONTABIL, NRBRM, INC_CONTABIL) VALUES (?,?,?,?,?,?);",
                depara_rows,
            )
        if conc_rows:
            cur.executemany(
                "INSERT OR IGNORE INTO conciliados (BASE, ID, PAR_ID) VALUES (?,?,?);",
                conc_rows,
            )

        cur.execute("COMMIT;")
        return saved

    except Exception:
        cur.execute("ROLLBACK;")
        raise


def save_manual_pairs(con: sqlite3.Connection, pairs: List[Tuple[int, int]]) -> int:
    # MANUAL: filhos recebem "CM - INC"
    return save_pairs_with_family(con, pairs, st_conciliacao="MANUAL")


def save_direct_pairs(con: sqlite3.Connection, pairs: List[Tuple[int, int]]) -> int:
    # DIRETA 1-para-1: filhos recebem "CD - INC"
    return save_pairs_with_family(con, pairs, st_conciliacao="DIRETA")


def save_nao_chapeavel_pairs(con: sqlite3.Connection, pairs: List[Tuple[int, int]]) -> int:
    # NÃO CHAPEÁVEL: pode vir (0, ctb_id) e filhos recebem "CN - INC"
    return save_pairs_with_family(con, pairs, st_conciliacao="NÃO CHAPEÁVEL")


# ---------------- Descotejar (DESCONCILIAR) ----------------

def undo_pairs(con: sqlite3.Connection, pairs: List[Tuple[int, int]]) -> Dict[str, int]:
    """Desfaz conciliações informadas.

    Regras (modo A do Everaldo):
      - Remove linhas da tabela 'depara' referentes aos IDs informados
      - Remove bloqueios correspondentes em 'conciliados' (volta a ficar pendente)
      - Se existir coluna FRAG nas tabelas base, limpa o valor (volta a ficar pendente)

    Aceita pares com um lado vazio (0):
      - (fis_id>0, ctb_id=0) => descoteja qualquer conciliação envolvendo esse fis_id
      - (fis_id=0, ctb_id>0) => descoteja qualquer conciliação envolvendo esse ctb_id
      - (fis_id>0, ctb_id>0) => descoteja o par exato
    """
    out: Dict[str, int] = {
        "removed_depara": 0,
        "removed_conc": 0,
        "unmarked_fis": 0,
        "unmarked_ctb": 0,
    }
    if not pairs:
        return out

    # normaliza e remove duplicados
    norm_pairs: List[Tuple[int, int]] = []
    seen = set()
    for a, b in pairs:
        fa = int(a or 0)
        cb = int(b or 0)
        key = (fa, cb)
        if key in seen:
            continue
        seen.add(key)
        if fa <= 0 and cb <= 0:
            continue
        norm_pairs.append(key)

    if not norm_pairs:
        return out

    cur = con.cursor()
    try:
        cur.execute("BEGIN;")

        # Descobre PAR_IDs a remover
        par_ids: set[int] = set()
        fis_ids: set[int] = set()
        ctb_ids: set[int] = set()

        for fis_id, ctb_id in norm_pairs:
            if fis_id > 0:
                fis_ids.add(fis_id)
            if ctb_id > 0:
                ctb_ids.add(ctb_id)

            if fis_id > 0 and ctb_id > 0:
                rows = cur.execute(
                    "SELECT PAR_ID FROM depara WHERE ID_FISICO=? AND ID_CONTABIL=?;",
                    (fis_id, ctb_id),
                ).fetchall()
                # Regra de família: se o CTB informado for PAI (INC=0), remove também filhos (INC!=0)
                # do mesmo NRBRM (mesmo ID_FISICO âncora).
                fam = cur.execute(
                    "SELECT COALESCE(NRBRM,0), COALESCE(INC,0) FROM contabil WHERE ID=?;",
                    (ctb_id,),
                ).fetchone()
                if fam:
                    nrbrm = int(fam[0] or 0)
                    inc = int(fam[1] or 0)
                    if nrbrm > 0 and inc == 0:
                        fam_rows = cur.execute(
                            "SELECT PAR_ID FROM depara WHERE COALESCE(NRBRM,0)=? AND ID_FISICO=?;",
                            (nrbrm, fis_id),
                        ).fetchall()
                        rows = list(rows) + list(fam_rows)
            elif fis_id > 0 and ctb_id <= 0:
                rows = cur.execute(
                    "SELECT PAR_ID FROM depara WHERE ID_FISICO=?;",
                    (fis_id,),
                ).fetchall()
            else:  # fis_id <=0 and ctb_id > 0
                rows = cur.execute(
                    "SELECT PAR_ID FROM depara WHERE ID_CONTABIL=?;",
                    (ctb_id,),
                ).fetchall()
                # Regra de família para não-chapeável: se CTB é PAI (INC=0), remove família pelo NRBRM.
                fam = cur.execute(
                    "SELECT COALESCE(NRBRM,0), COALESCE(INC,0) FROM contabil WHERE ID=?;",
                    (ctb_id,),
                ).fetchone()
                if fam:
                    nrbrm = int(fam[0] or 0)
                    inc = int(fam[1] or 0)
                    if nrbrm > 0 and inc == 0:
                        fam_rows = cur.execute(
                            "SELECT PAR_ID FROM depara WHERE COALESCE(NRBRM,0)=?;",
                            (nrbrm,),
                        ).fetchall()
                        rows = list(rows) + list(fam_rows)

            for (pid,) in rows:
                try:
                    par_ids.add(int(pid))
                except Exception:
                    pass

        # Inclui IDs realmente impactados (do(s) PAR_ID(s) encontrados) para limpar FRAG com consistência.
        if par_ids:
            ph = ",".join(["?"] * len(par_ids))
            impacted = cur.execute(
                f"SELECT COALESCE(ID_FISICO,0), COALESCE(ID_CONTABIL,0) FROM depara WHERE PAR_ID IN ({ph});",
                tuple(sorted(par_ids)),
            ).fetchall()
            for f_id, c_id in impacted:
                f_id = int(f_id or 0)
                c_id = int(c_id or 0)
                if f_id > 0:
                    fis_ids.add(f_id)
                if c_id > 0:
                    ctb_ids.add(c_id)

        # Remove depara
        if par_ids:
            ph = ",".join(["?"] * len(par_ids))
            cur.execute(f"DELETE FROM depara WHERE PAR_ID IN ({ph});", tuple(sorted(par_ids)))
            out["removed_depara"] = int(cur.rowcount if cur.rowcount is not None else 0)

        # Remove bloqueios em 'conciliados' (por PAR_ID e por IDs)
        removed_conc = 0
        if par_ids:
            ph = ",".join(["?"] * len(par_ids))
            cur.execute(f"DELETE FROM conciliados WHERE PAR_ID IN ({ph});", tuple(sorted(par_ids)))
            removed_conc += int(cur.rowcount if cur.rowcount is not None else 0)

        if fis_ids:
            ph = ",".join(["?"] * len(fis_ids))
            cur.execute(f"DELETE FROM conciliados WHERE BASE='FIS' AND ID IN ({ph});", tuple(sorted(fis_ids)))
            removed_conc += int(cur.rowcount if cur.rowcount is not None else 0)

        if ctb_ids:
            ph = ",".join(["?"] * len(ctb_ids))
            cur.execute(f"DELETE FROM conciliados WHERE BASE='CTB' AND ID IN ({ph});", tuple(sorted(ctb_ids)))
            removed_conc += int(cur.rowcount if cur.rowcount is not None else 0)

        out["removed_conc"] = removed_conc

        # Limpa FRAG nas tabelas base se existir
        frag_f = _resolve_column(con, "fisico", ["FRAG"], contains="FRAG")
        if frag_f and fis_ids:
            ph = ",".join(["?"] * len(fis_ids))
            cur.execute(f"UPDATE fisico SET {frag_f}=NULL WHERE ID IN ({ph});", tuple(sorted(fis_ids)))
            out["unmarked_fis"] = int(cur.rowcount if cur.rowcount is not None else 0)

        frag_c = _resolve_column(con, "contabil", ["FRAG"], contains="FRAG")
        if frag_c and ctb_ids:
            ph = ",".join(["?"] * len(ctb_ids))
            cur.execute(f"UPDATE contabil SET {frag_c}=NULL WHERE ID IN ({ph});", tuple(sorted(ctb_ids)))
            out["unmarked_ctb"] = int(cur.rowcount if cur.rowcount is not None else 0)

        cur.execute("COMMIT;")
        return out
    except Exception:
        cur.execute("ROLLBACK;")
        raise


def find_children_ctb_ids(
    con: sqlite3.Connection,
    *,
    nrbrm: int,
    exclude_ctb_id: int,
    limit: int = 2000,
) -> List[int]:
    """Retorna IDs contábeis (pendentes) com mesmo NRBRM e INC != 0.

    - Exclui o ID do pai já selecionado.
    - Respeita pendência (não existe em conciliados).
    """
    q = """
    SELECT t.ID
    FROM contabil t
    WHERE t.NRBRM = ?
      AND COALESCE(t.INC,0) <> 0
      AND t.ID <> ?
      AND NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='CTB' AND c.ID=t.ID)
    ORDER BY t.ID
    LIMIT ?;
    """
    rows = con.execute(q, (int(nrbrm), int(exclude_ctb_id), int(limit))).fetchall()
    return [int(r[0]) for r in rows]

def load_pairs_auto02(
    con: sqlite3.Connection,
    rule_id: str,
    *,
    limit_pairs: int = 500,
    candidate_cap: int = 5000,
    desc1: str = "",
    desc2: str = "",
    desc3: str = "",
    desc_mode: str = "E",
    filial: str = "",
    ccusto: str = "",
    local: str = "",
    condic: str = "",
    data_ctb_ano: str = "",
) -> Tuple[pd.DataFrame, pd.DataFrame, List[Tuple[int,int]]]:
    """Automático (02) pareado por linha (FIS x CTB).

    Retorna (df_fis, df_ctb, pairs) onde:
      - df_fis e df_ctb têm o MESMO tamanho e mesma ordem;
      - pairs = [(fis_id, ctb_id), ...] na ordem exibida;
      - CTB vem somente com INC=0 (itens pai);
      - cada CTB é usado no máximo 1 vez (pareamento guloso).
    """
    df_f, df_c = load_candidates_auto02(
        con,
        rule_id,
        limit_each=int(candidate_cap),
        desc1=desc1, desc2=desc2, desc3=desc3, desc_mode=desc_mode,
        filial=filial, ccusto=ccusto, local=local, condic=condic,
        data_ctb_ano=data_ctb_ano,
    )
    if df_f.empty or df_c.empty:
        return pd.DataFrame(), pd.DataFrame(), []

    # Busca candidatos FIS x CTB (INC=0 já é garantido no SQL do auto02)
    rid = (rule_id or "").strip()

    if rid == "6":
        # Pareamento guloso por similaridade de descrição (>=2 atributos), usando candidatos já filtrados
        # monta atributos dos contábeis e índice invertido
        inv: Dict[str, List[int]] = {}
        c_attrs: Dict[int, set] = {}
        for _, row in df_c.iterrows():
            cid = int(row.get("ID"))
            attrs = _desc_attr_set(row.get("DESCRICAO") or row.get("DESC") or row.get("DESC_NORM") or "")
            c_attrs[cid] = attrs
            for a in attrs:
                inv.setdefault(a, []).append(cid)

        used_ctb = set()
        pairs: List[Tuple[int, int]] = []
        fis_rows = []
        ctb_rows = []

        for _, row in df_f.iterrows():
            fid = int(row.get("ID"))
            fattrs = _desc_attr_set(row.get("DESCRICAO") or row.get("DESC") or row.get("DESC_NORM") or "")
            if not fattrs:
                continue

            counts: Dict[int, int] = {}
            for a in fattrs:
                for cid in inv.get(a, []):
                    if cid in used_ctb:
                        continue
                    counts[cid] = counts.get(cid, 0) + 1

            # escolhe o melhor match disponível com >=2 atributos
            best = None
            best_score = 0
            for cid, score in counts.items():
                if score >= 2 and score > best_score:
                    best = cid
                    best_score = score

            if best is None:
                continue

            used_ctb.add(best)
            pairs.append((fid, best))

        if not pairs:
            return pd.DataFrame(), pd.DataFrame(), []

        # monta dfs alinhados na ordem de pairs
        fis_map = df_f.set_index("ID")
        ctb_map = df_c.set_index("ID")
        for fid, cid in pairs[: int(limit_pairs)]:
            if fid in fis_map.index and cid in ctb_map.index:
                fis_rows.append(fis_map.loc[fid])
                ctb_rows.append(ctb_map.loc[cid])

        df_f_out = pd.DataFrame(fis_rows).reset_index()
        df_c_out = pd.DataFrame(ctb_rows).reset_index()
        return df_f_out, df_c_out, pairs[: int(limit_pairs)]

    # Recria a mesma condição de join do auto02
    if rid == "1":
        join_cond = "LENGTH(f.SERIE_NORM) >= 4 AND t.DESC_NORM LIKE ('%' || f.SERIE_NORM || '%')"
    elif rid == "2":
        join_cond = "LENGTH(t.SERIE_NORM) >= 4 AND f.DESC_NORM LIKE ('%' || t.SERIE_NORM || '%')"
    elif rid == "3":
        join_cond = "t.MODELO_NORM <> '' AND f.MODELO_NORM = t.MODELO_NORM"
    elif rid == "4":
        join_cond = "LENGTH(f.MODELO_NORM) >= 4 AND t.DESC_NORM LIKE ('%' || f.MODELO_NORM || '%')"
    else:  # "5"
        join_cond = "LENGTH(f.TAG_NORM) >= 4 AND t.DESC_NORM LIKE ('%' || f.TAG_NORM || '%')"

    w_fis, p_fis = _auto02_base_filters("f", desc1=desc1, desc2=desc2, desc3=desc3, desc_mode=desc_mode,
                                       filial=filial, ccusto=ccusto, local=local, condic=condic)
    w_ctb, p_ctb = _auto02_base_filters("t", desc1=desc1, desc2=desc2, desc3=desc3, desc_mode=desc_mode,
                                       filial=filial, ccusto=ccusto, local=local, condic="")

    if (data_ctb_ano or "").strip():
        ano = (data_ctb_ano or "").strip()
        if len(ano) >= 4:
            ano = ano[:4]
        w_ctb += " AND SUBSTR(COALESCE(t.DT_AQUISICAO,''),1,4) = ? "
        p_ctb.append(ano)

    pend_fis = "NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='FIS' AND c.ID=f.ID)"
    pend_ctb = "NOT EXISTS (SELECT 1 FROM conciliados c WHERE c.BASE='CTB' AND c.ID=t.ID)"

    base_sql = f"""
    fisico f
    JOIN contabil t ON ({join_cond})
    WHERE f.ID IS NOT NULL AND t.ID IS NOT NULL
      AND {pend_fis}
      AND {pend_ctb}
      AND COALESCE(t.INC,0) = 0
      {w_fis}
      {w_ctb}
    """

    q_pairs = f"SELECT f.ID AS FIS_ID, t.ID AS CTB_ID FROM {base_sql} ORDER BY f.ID, t.ID LIMIT ?;"
    params_join = p_fis + p_ctb
    cand = pd.read_sql_query(q_pairs, con, params=params_join + [int(candidate_cap)])
    if cand.empty:
        return pd.DataFrame(), pd.DataFrame(), []

    used_ctb: set[int] = set()
    pairs: List[Tuple[int,int]] = []
    for fis_id, grp in cand.groupby("FIS_ID", sort=True):
        for ctb_id in grp["CTB_ID"].tolist():
            ctb_id = int(ctb_id)
            if ctb_id in used_ctb:
                continue
            used_ctb.add(ctb_id)
            pairs.append((int(fis_id), ctb_id))
            break
        if len(pairs) >= int(limit_pairs):
            break

    if not pairs:
        return pd.DataFrame(), pd.DataFrame(), []

    fis_ids = [p[0] for p in pairs]
    ctb_ids = [p[1] for p in pairs]

    df_f_full = pd.read_sql_query(
        f"SELECT * FROM fisico WHERE ID IN ({','.join(['?']*len(fis_ids))});",
        con,
        params=[int(x) for x in fis_ids],
    )
    df_c_full = pd.read_sql_query(
        f"SELECT * FROM contabil WHERE ID IN ({','.join(['?']*len(ctb_ids))});",
        con,
        params=[int(x) for x in ctb_ids],
    )

    # Reordena conforme pairs
    df_f_full["__ord"] = df_f_full["ID"].map({fid:i for i,fid in enumerate(fis_ids)})
    df_c_full["__ord"] = df_c_full["ID"].map({cid:i for i,cid in enumerate(ctb_ids)})
    df_f_full = df_f_full.sort_values("__ord").drop(columns=["__ord"]).reset_index(drop=True)
    df_c_full = df_c_full.sort_values("__ord").drop(columns=["__ord"]).reset_index(drop=True)
    return df_f_full, df_c_full, pairs




def generate_pre_depara_direct(con: sqlite3.Connection) -> int:
    """
    Gera sugestões de conciliação direta por NRBRM igual (INC livre) e grava em pre_depara como PENDENTE.
    Ignora IDs já conciliados e pares já sugeridos.
    Retorna quantidade inserida.
    """
    cur = con.cursor()
    cur.execute("BEGIN;")
    try:
        cur.execute("""
            INSERT OR IGNORE INTO pre_depara (ID_FISICO, ID_CONTABIL, NRBRM, INC_CONTABIL)
            SELECT f.ID, c.ID, f.NRBRM, c.INC
            FROM fisico f
            JOIN contabil c ON c.NRBRM = f.NRBRM
            LEFT JOIN conciliados cf ON (cf.BASE='FIS' AND cf.ID=f.ID)
            LEFT JOIN conciliados cc ON (cc.BASE='CTB' AND cc.ID=c.ID)
            WHERE cf.ID IS NULL
              AND cc.ID IS NULL
              AND f.NRBRM IS NOT NULL;
        """)
        inserted = cur.rowcount if cur.rowcount is not None else 0
        cur.execute("COMMIT;")
        return int(inserted)
    except Exception:
        cur.execute("ROLLBACK;")
        raise


def fetch_pre_depara(con: sqlite3.Connection, status: str = "PENDENTE") -> list[tuple]:
    cur = con.cursor()
    return cur.execute(
        "SELECT SUG_ID, ID_FISICO, ID_CONTABIL FROM pre_depara WHERE STATUS=? ORDER BY SUG_ID;",
        (status,),
    ).fetchall()


def set_pre_depara_status(con: sqlite3.Connection, sug_id: int, status: str) -> None:
    cur = con.cursor()
    cur.execute("UPDATE pre_depara SET STATUS=? WHERE SUG_ID=?;", (status, sug_id))
    con.commit()


def commit_pre_depara_aprovados(con: sqlite3.Connection) -> int:
    cur = con.cursor()
    rows = cur.execute(
        "SELECT ID_FISICO, ID_CONTABIL FROM pre_depara WHERE STATUS='APROVADO' ORDER BY SUG_ID;"
    ).fetchall()
    saved = save_direct_pairs(con, [(int(a), int(b)) for a, b in rows])

    cur.execute("""
        DELETE FROM pre_depara
        WHERE STATUS='APROVADO'
          AND ID_FISICO IN (SELECT ID FROM conciliados WHERE BASE='FIS')
          AND ID_CONTABIL IN (SELECT ID FROM conciliados WHERE BASE='CTB');
    """)
    con.commit()
    return saved
