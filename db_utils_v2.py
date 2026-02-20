import os
import re
import sqlite3
from contextlib import contextmanager

try:
    import psycopg2
    PG_AVAILABLE = True
except Exception:
    PG_AVAILABLE = False


SCHEMA_VERSION = 2
_AUTO_BACKEND: str | None = None
_AUTO_PG_DSN: str | None = None


def _qmark_to_pyformat(sql: str) -> str:
    out: list[str] = []
    in_single = False
    in_double = False
    for ch in sql:
        if ch == "'" and not in_double:
            in_single = not in_single
            out.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            out.append(ch)
            continue
        if ch == "?" and not in_single and not in_double:
            out.append("%s")
        else:
            out.append(ch)
    return "".join(out)


def _rewrite_insert_or_ignore(sql: str) -> str:
    if not re.search(r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+", sql, flags=re.IGNORECASE):
        return sql
    rewritten = re.sub(
        r"^\s*INSERT\s+OR\s+IGNORE\s+INTO\s+",
        "INSERT INTO ",
        sql,
        flags=re.IGNORECASE,
    )
    stripped = rewritten.rstrip()
    if stripped.endswith(";"):
        stripped = stripped[:-1]
    return stripped + " ON CONFLICT DO NOTHING;"


def _normalize_pg_dsn(dsn: str) -> str:
    dsn = (dsn or "").strip()
    if not dsn:
        return ""
    if "connect_timeout=" not in dsn:
        dsn = f"{dsn} connect_timeout=2"
    return dsn


def _evs_pg_dsn_from_env() -> str:
    host = os.getenv("EVS_PG_HOST", "").strip()
    port = os.getenv("EVS_PG_PORT", "").strip()
    dbname = os.getenv("EVS_PG_DB", "").strip()
    user = os.getenv("EVS_PG_USER", "").strip()
    password = os.getenv("EVS_PG_PASSWORD", "").strip()
    if not any([host, port, dbname, user, password]):
        return ""
    host = host or "localhost"
    port = port or "5432"
    dbname = dbname or "evs_conciliador"
    user = user or "evs"
    password = password or "evs123"
    return _normalize_pg_dsn(
        f"host={host} port={port} dbname={dbname} user={user} password={password}"
    )


def _pg_candidates() -> list[str]:
    dsns: list[str] = []
    pg_dsn = _normalize_pg_dsn(os.getenv("PG_DSN", ""))
    if pg_dsn:
        dsns.append(pg_dsn)

    env_dsn = _evs_pg_dsn_from_env()
    if env_dsn and env_dsn not in dsns:
        dsns.append(env_dsn)

    defaults = [
        "host=localhost port=5432 dbname=evs_conciliador user=evs password=evs123",
        "host=localhost port=5432 dbname=evs_conciliador user=postgres password=postgres",
    ]
    os_user = (os.getenv("USER") or "").strip()
    if os_user:
        defaults.append(
            f"host=localhost port=5432 dbname=evs_conciliador user={os_user}"
        )

    for d in defaults:
        nd = _normalize_pg_dsn(d)
        if nd not in dsns:
            dsns.append(nd)
    return dsns


class CompatCursor:
    def __init__(self, raw_cursor, conn: "CompatConnection"):
        self._raw = raw_cursor
        self._conn = conn
        self._rows_override = None
        self._row_idx = 0

    def _set_rows_override(self, rows):
        self._rows_override = list(rows)
        self._row_idx = 0

    def _use_override(self) -> bool:
        return self._rows_override is not None

    def execute(self, sql, params=None):
        params = tuple(params or ())
        self._rows_override = None
        self._row_idx = 0
        if self._conn._handle_special_sql(self, sql, params):
            return self
        self._raw.execute(self._conn._rewrite_sql(sql), params)
        return self

    def executemany(self, sql, seq_of_params):
        self._rows_override = None
        self._row_idx = 0
        self._raw.executemany(self._conn._rewrite_sql(sql), seq_of_params)
        return self

    def fetchone(self):
        if not self._use_override():
            return self._raw.fetchone()
        if self._row_idx >= len(self._rows_override):
            return None
        row = self._rows_override[self._row_idx]
        self._row_idx += 1
        return row

    def fetchmany(self, size=None):
        if not self._use_override():
            return self._raw.fetchmany(size)
        if size is None:
            size = 1
        start = self._row_idx
        end = min(len(self._rows_override), self._row_idx + int(size))
        self._row_idx = end
        return self._rows_override[start:end]

    def fetchall(self):
        if not self._use_override():
            return self._raw.fetchall()
        start = self._row_idx
        self._row_idx = len(self._rows_override)
        return self._rows_override[start:]

    def close(self):
        return self._raw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __getattr__(self, item):
        return getattr(self._raw, item)


class CompatConnection:
    def __init__(self, raw_conn, backend: str):
        self._raw = raw_conn
        self._evs_backend = backend

    def _rewrite_sql(self, sql: str) -> str:
        if self._evs_backend != "postgres":
            return sql
        rewritten = _rewrite_insert_or_ignore(sql)
        rewritten = re.sub(r"\bd\.rowid\b", "d.par_id", rewritten, flags=re.IGNORECASE)
        return _qmark_to_pyformat(rewritten)

    def _handle_special_sql(self, cur: CompatCursor, sql: str, params: tuple) -> bool:
        if self._evs_backend != "postgres":
            return False
        norm = (sql or "").strip()
        low = norm.lower()
        if low.startswith("pragma "):
            m = re.search(r"pragma\s+table_info\(([^)]+)\)", low, flags=re.IGNORECASE)
            if m:
                table = m.group(1).strip().strip("'\"")
                cur._raw.execute(
                    """
                    SELECT
                        (ordinal_position - 1) AS cid,
                        column_name AS name,
                        data_type AS type,
                        CASE WHEN is_nullable='NO' THEN 1 ELSE 0 END AS notnull,
                        column_default AS dflt_value,
                        0 AS pk
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name=%s
                    ORDER BY ordinal_position
                    """,
                    (table,),
                )
                cur._set_rows_override(cur._raw.fetchall())
            else:
                cur._set_rows_override([])
            return True

        if "from sqlite_master" in low:
            if "and name=?" in low and params:
                cur._raw.execute(
                    """
                    SELECT table_name AS name
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_name=%s
                    ORDER BY table_name
                    """,
                    (params[0],),
                )
            else:
                cur._raw.execute(
                    """
                    SELECT table_name AS name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    ORDER BY table_name
                    """
                )
            cur._set_rows_override(cur._raw.fetchall())
            return True
        return False

    def cursor(self):
        if self._evs_backend == "postgres":
            return CompatCursor(self._raw.cursor(), self)
        return self._raw.cursor()

    def execute(self, sql, params=None):
        cur = self.cursor()
        cur.execute(sql, params or ())
        return cur

    def executemany(self, sql, seq_of_params):
        cur = self.cursor()
        cur.executemany(sql, seq_of_params)
        return cur

    def commit(self):
        return self._raw.commit()

    def rollback(self):
        return self._raw.rollback()

    def close(self):
        return self._raw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        finally:
            self.close()
        return False

    def __getattr__(self, item):
        return getattr(self._raw, item)


def _is_postgres(conn):
    if getattr(conn, "_evs_backend", "") == "postgres":
        return True
    return PG_AVAILABLE and conn.__class__.__module__.startswith("psycopg2")


def connect(db_path: str):
    global _AUTO_BACKEND, _AUTO_PG_DSN

    forced = os.getenv("EVS_DB_BACKEND", "").strip().lower()
    if forced == "sqlite":
        _AUTO_BACKEND = "sqlite"
        return sqlite3.connect(db_path)

    if forced == "postgres":
        if not PG_AVAILABLE:
            raise RuntimeError(
                "EVS_DB_BACKEND=postgres definido, mas psycopg2 não está disponível."
            )
        last_exc = None
        for dsn in _pg_candidates():
            try:
                conn = psycopg2.connect(dsn)
                _AUTO_BACKEND = "postgres"
                _AUTO_PG_DSN = dsn
                return CompatConnection(conn, "postgres")
            except Exception as e:
                last_exc = e
                continue
        raise RuntimeError(
            "EVS_DB_BACKEND=postgres definido, mas nenhuma conexão PostgreSQL foi estabelecida."
        ) from last_exc

    if _AUTO_BACKEND == "postgres" and _AUTO_PG_DSN and PG_AVAILABLE:
        try:
            return CompatConnection(psycopg2.connect(_AUTO_PG_DSN), "postgres")
        except Exception:
            _AUTO_BACKEND = None
            _AUTO_PG_DSN = None

    if _AUTO_BACKEND == "sqlite":
        return sqlite3.connect(db_path)

    if PG_AVAILABLE:
        for dsn in _pg_candidates():
            try:
                conn = psycopg2.connect(dsn)
                _AUTO_BACKEND = "postgres"
                _AUTO_PG_DSN = dsn
                return CompatConnection(conn, "postgres")
            except Exception:
                continue

    _AUTO_BACKEND = "sqlite"
    _AUTO_PG_DSN = None
    return sqlite3.connect(db_path)


def init_db(conn):
    cur = conn.cursor()

    if _is_postgres(conn):
        # Tabelas base (compatíveis com o fluxo SQLFAST/manual/export).
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fisico (
            row_id BIGSERIAL PRIMARY KEY,
            ID BIGINT,
            COD_CONTA TEXT,
            DESC_CONTA TEXT,
            FILIAL BIGINT,
            DESC_FILIAL TEXT,
            CCUSTO TEXT,
            DESCR_CCUSTO TEXT,
            LOCAL BIGINT,
            DESCR_LOCAL TEXT,
            NRBRM BIGINT,
            INC BIGINT,
            DESCRICAO TEXT,
            MARCA TEXT,
            MODELO TEXT,
            SERIE TEXT,
            DIMENSAO TEXT,
            CAPACIDADE TEXT,
            TAG TEXT,
            BEM_ANTERIOR TEXT,
            CONDIC TEXT,
            QTD BIGINT,
            FRAG TEXT,
            DESC_NORM TEXT,
            MARCA_NORM TEXT,
            MODELO_NORM TEXT,
            SERIE_NORM TEXT,
            TAG_NORM TEXT,
            BEM_ANT_NORM TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS contabil (
            row_id BIGSERIAL PRIMARY KEY,
            ID BIGINT,
            COD_CONTA TEXT,
            DESC_CONTA TEXT,
            FILIAL BIGINT,
            DESC_FILIAL TEXT,
            CCUSTO TEXT,
            DESCR_CCUSTO TEXT,
            LOCAL BIGINT,
            DESCR_LOCAL TEXT,
            NRBRM BIGINT,
            INC BIGINT,
            DESCRICAO TEXT,
            MARCA TEXT,
            MODELO TEXT,
            SERIE TEXT,
            DIMENSAO TEXT,
            CAPACIDADE TEXT,
            TAG TEXT,
            BEM_ANTERIOR TEXT,
            QTD BIGINT,
            DT_AQUISICAO TEXT,
            VLR_AQUISICAO DOUBLE PRECISION,
            DEP_ACUMULADA DOUBLE PRECISION,
            VLR_RESIDUAL DOUBLE PRECISION,
            FRAG TEXT,
            DESC_NORM TEXT,
            MARCA_NORM TEXT,
            MODELO_NORM TEXT,
            SERIE_NORM TEXT,
            TAG_NORM TEXT,
            BEM_ANT_NORM TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS depara (
            par_id BIGSERIAL PRIMARY KEY,
            st_conciliacao TEXT NOT NULL,
            id_fisico BIGINT NOT NULL DEFAULT 0,
            id_contabil BIGINT NOT NULL DEFAULT 0,
            nrbrm BIGINT,
            inc_contabil BIGINT,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS conciliados (
            base TEXT,
            id BIGINT,
            par_id BIGINT
        );
        """)
        cur.execute("""
        ALTER TABLE conciliados
        ADD COLUMN IF NOT EXISTS id BIGINT;
        """)
        cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='conciliados'
                  AND column_name='id'
                  AND data_type <> 'bigint'
            ) THEN
                ALTER TABLE conciliados
                ALTER COLUMN id TYPE BIGINT
                USING NULLIF(id::text,'')::BIGINT;
            END IF;
        END $$;
        """)
        cur.execute("""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema='public'
                  AND table_name='conciliados'
                  AND column_name='base_id'
            ) THEN
                UPDATE conciliados
                   SET id = NULLIF(base_id,'')::BIGINT
                 WHERE id IS NULL
                   AND base_id ~ '^[0-9]+$';
            END IF;
        END $$;
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_conc_base_id
        ON conciliados(base, id);
        """)
        cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_conc_base_id
        ON conciliados(base, id);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_fis_id ON fisico(id);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_ctb_id ON contabil(id);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_depara_fis ON depara(id_fisico);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_depara_ctb ON depara(id_contabil);
        """)

        cur.execute("""
        INSERT INTO meta(k, v)
        VALUES ('schema_version', %s)
        ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v;
        """, (str(SCHEMA_VERSION),))

    else:
        # Tabelas base (schema normalizado usado por import/manual/automático/export).
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fisico (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ID INTEGER,
            COD_CONTA TEXT,
            DESC_CONTA TEXT,
            FILIAL INTEGER,
            DESC_FILIAL TEXT,
            CCUSTO TEXT,
            DESCR_CCUSTO TEXT,
            LOCAL INTEGER,
            DESCR_LOCAL TEXT,
            NRBRM INTEGER,
            INC INTEGER,
            DESCRICAO TEXT,
            MARCA TEXT,
            MODELO TEXT,
            SERIE TEXT,
            DIMENSAO TEXT,
            CAPACIDADE TEXT,
            TAG TEXT,
            BEM_ANTERIOR TEXT,
            CONDIC TEXT,
            QTD INTEGER,
            FRAG TEXT,
            DESC_NORM TEXT,
            MARCA_NORM TEXT,
            MODELO_NORM TEXT,
            SERIE_NORM TEXT,
            TAG_NORM TEXT,
            BEM_ANT_NORM TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS contabil (
            row_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ID INTEGER,
            COD_CONTA TEXT,
            DESC_CONTA TEXT,
            FILIAL INTEGER,
            DESC_FILIAL TEXT,
            CCUSTO TEXT,
            DESCR_CCUSTO TEXT,
            LOCAL INTEGER,
            DESCR_LOCAL TEXT,
            NRBRM INTEGER,
            INC INTEGER,
            DESCRICAO TEXT,
            MARCA TEXT,
            MODELO TEXT,
            SERIE TEXT,
            DIMENSAO TEXT,
            CAPACIDADE TEXT,
            TAG TEXT,
            BEM_ANTERIOR TEXT,
            QTD INTEGER,
            DT_AQUISICAO TEXT,
            VLR_AQUISICAO REAL,
            DEP_ACUMULADA REAL,
            VLR_RESIDUAL REAL,
            FRAG TEXT,
            DESC_NORM TEXT,
            MARCA_NORM TEXT,
            MODELO_NORM TEXT,
            SERIE_NORM TEXT,
            TAG_NORM TEXT,
            BEM_ANT_NORM TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS depara (
            PAR_ID INTEGER PRIMARY KEY AUTOINCREMENT,
            ST_CONCILIACAO TEXT NOT NULL,
            ID_FISICO INTEGER NOT NULL DEFAULT 0,
            ID_CONTABIL INTEGER NOT NULL DEFAULT 0,
            NRBRM INTEGER,
            INC_CONTABIL INTEGER,
            CREATED_AT TEXT DEFAULT (datetime('now','localtime'))
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            k TEXT PRIMARY KEY,
            v TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS conciliados (
            BASE TEXT,
            ID TEXT,
            PAR_ID INTEGER
        );
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_conc_base_id
        ON conciliados(BASE, ID);
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_fis_id ON fisico(ID);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ctb_id ON contabil(ID);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_depara_fis ON depara(ID_FISICO);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_depara_ctb ON depara(ID_CONTABIL);")

        cur.execute("""
        INSERT OR REPLACE INTO meta(k, v)
        VALUES ('schema_version', ?);
        """, (str(SCHEMA_VERSION),))

    conn.commit()


def fetchone(conn, sql, params=None):
    cur = conn.cursor()
    cur.execute(sql, params or ())
    return cur.fetchone()


def fetchval(conn, sql, params=None, default=None):
    row = fetchone(conn, sql, params)
    return row[0] if row else default


@contextmanager
def transaction(conn):
    try:
        yield
        conn.commit()
    except Exception:
        conn.rollback()
        raise
