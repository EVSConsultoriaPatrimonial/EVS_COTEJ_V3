# db_utils_pg.py (FIXED) - EVS PostgreSQL utilities (local)
# Target: PostgreSQL 12+ (works on 18)
# Notes:
# - This module expects the database to exist (e.g., evs_conciliador).
# - The application user (evs) must have CREATE privilege on schema public.
#   If not, init_pg() will raise a clear error with the SQL to fix it.

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Sequence

import psycopg2
import psycopg2.extras


@dataclass(frozen=True)
class PgConfig:
    host: str = "localhost"
    port: int = 5432
    dbname: str = "evs_conciliador"
    user: str = "evs"
    password: str = "evs123"

    @staticmethod
    def from_env(prefix: str = "EVS_PG_") -> "PgConfig":
        return PgConfig(
            host=os.getenv(prefix + "HOST", "localhost"),
            port=int(os.getenv(prefix + "PORT", "5432")),
            dbname=os.getenv(prefix + "DB", "evs_conciliador"),
            user=os.getenv(prefix + "USER", "evs"),
            password=os.getenv(prefix + "PASSWORD", "evs123"),
        )

    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


SCHEMA_SQL = '''
CREATE TABLE IF NOT EXISTS "BsFisico"   (__dummy__ TEXT);
CREATE TABLE IF NOT EXISTS "BsContabil" (__dummy__ TEXT);
CREATE TABLE IF NOT EXISTS "BsDePara"   (__dummy__ TEXT);

CREATE TABLE IF NOT EXISTS conciliados (
    BASE   TEXT NOT NULL,
    BASE_ID TEXT NOT NULL,
    PRIMARY KEY (BASE, BASE_ID)
);
'''


def _raise_privilege_help(err: Exception, cfg: PgConfig) -> None:
    msg = f'''
[EVS] PostgreSQL permission error: {err}

Your PostgreSQL user "{cfg.user}" does NOT have permission to CREATE objects in schema public.

Fix (run ONCE inside psql as user "postgres"):

  \\c {cfg.dbname}
  GRANT USAGE, CREATE ON SCHEMA public TO {cfg.user};
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {cfg.user};
  ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO {cfg.user};

After that, run the migration again.
'''
    raise RuntimeError(msg) from err


def connect_pg(cfg: Optional[PgConfig] = None):
    cfg = cfg or PgConfig.from_env()
    return psycopg2.connect(cfg.dsn())


def init_pg(cfg_or_dsn: Optional[str | PgConfig] = None) -> None:
    if isinstance(cfg_or_dsn, str):
        cfg = PgConfig.from_env()
        dsn = cfg_or_dsn
    else:
        cfg = cfg_or_dsn or PgConfig.from_env()
        dsn = cfg.dsn()

    try:
        with psycopg2.connect(dsn) as con:
            with con.cursor() as cur:
                cur.execute(SCHEMA_SQL)
            con.commit()
    except psycopg2.errors.InsufficientPrivilege as e:
        _raise_privilege_help(e, cfg)


def ensure_table_columns(cur, table: str, columns: Sequence[str]) -> None:
    cur.execute(
        '''
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ''',
        (table,),
    )
    existing = {r[0] for r in cur.fetchall()}
    for c in columns:
        if c not in existing:
            cur.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" TEXT;')


def create_indexes(cur) -> None:
    cur.execute('CREATE INDEX IF NOT EXISTS idx_conciliados_base_id ON conciliados(BASE, BASE_ID);')

    for t in ("BsFisico", "BsContabil"):
        # Only create if columns exist
        idx_specs = [
            (("FRAG",), f"idx_{t.lower()}_frag"),
            (("NRBRM", "INC"), f"idx_{t.lower()}_nrbm_inc"),
            (("FILIAL", "CCUSTO", "LOCAL"), f"idx_{t.lower()}_filial_ccusto_local"),
        ]
        for cols, idx_name in idx_specs:
            cur.execute(
                '''
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s AND column_name = ANY(%s)
                ''',
                (t, list(cols)),
            )
            cnt = cur.fetchone()[0]
            if cnt == len(cols):
                cols_sql = ", ".join([f'"{c}"' for c in cols])
                cur.execute(f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{t}" ({cols_sql});')
