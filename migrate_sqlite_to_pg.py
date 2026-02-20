#!/usr/bin/env python3
# migrate_sqlite_to_pg.py (FIXED)
# Migrate EVS SQLite (conciliador.db) -> PostgreSQL local (evs_conciliador)
#
# Usage:
#   python migrate_sqlite_to_pg.py --sqlite conciliador.db --drop
#
# Env for PG connection:
#   EVS_PG_HOST, EVS_PG_PORT, EVS_PG_DB, EVS_PG_USER, EVS_PG_PASSWORD

from __future__ import annotations

import argparse
import sqlite3
from typing import List, Tuple

import psycopg2
import psycopg2.extras

from db_utils_pg import PgConfig, init_pg, ensure_table_columns, create_indexes


def _sqlite_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [r[0] for r in cur.fetchall()]


def _sqlite_table_cols(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(f'PRAGMA table_info("{table}")')
    return [r[1] for r in cur.fetchall()]


def _pg_drop_tables(pg, tables: List[str]) -> None:
    with pg.cursor() as cur:
        for t in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{t}" CASCADE;')
        cur.execute("DROP TABLE IF EXISTS conciliados CASCADE;")
    pg.commit()


def _copy_table(sqlite_conn: sqlite3.Connection, pg, table: str, batch_size: int = 5000) -> int:
    cols = _sqlite_table_cols(sqlite_conn, table)
    if not cols:
        return 0

    with pg.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" (__dummy__ TEXT);')
        ensure_table_columns(cur, table, cols)
    pg.commit()

    scur = sqlite_conn.cursor()
    select_cols = ", ".join([f'"{c}"' for c in cols])
    scur.execute(f'SELECT {select_cols} FROM "{table}"')

    insert_cols = ", ".join([f'"{c}"' for c in cols])
    insert_sql = f'INSERT INTO "{table}" ({insert_cols}) VALUES %s'

    inserted = 0
    with pg.cursor() as cur:
        while True:
            rows = scur.fetchmany(batch_size)
            if not rows:
                break
            psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=batch_size)
            inserted += len(rows)
    pg.commit()
    return inserted


def _migrate_conciliados(sqlite_conn: sqlite3.Connection, pg) -> int:
    tables = set(_sqlite_tables(sqlite_conn))
    if "conciliados" not in tables:
        return 0

    cols = _sqlite_table_cols(sqlite_conn, "conciliados")
    base_col = "BASE" if "BASE" in cols else ("base" if "base" in cols else None)
    id_col = None
    for cand in ("BASE_ID", "base_id", "ID", "id"):
        if cand in cols:
            id_col = cand
            break
    if not base_col or not id_col:
        return 0

    scur = sqlite_conn.cursor()
    scur.execute(f'SELECT "{base_col}", "{id_col}" FROM "conciliados"')
    rows = scur.fetchall()

    with pg.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS conciliados(
                BASE TEXT NOT NULL,
                BASE_ID TEXT NOT NULL,
                PRIMARY KEY (BASE, BASE_ID)
            );
        ''')
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO conciliados(BASE, BASE_ID) VALUES %s ON CONFLICT DO NOTHING",
            rows,
            page_size=5000,
        )
    pg.commit()
    return len(rows)


def migrate(sqlite_path: str, drop_and_recreate: bool = False) -> None:
    cfg = PgConfig.from_env()

    # Ensure schema exists (will raise helpful error if privileges missing)
    init_pg(cfg)

    sqlite_conn = sqlite3.connect(sqlite_path)
    with psycopg2.connect(cfg.dsn()) as pg:
        pg.autocommit = False

        if drop_and_recreate:
            _pg_drop_tables(pg, ["BsFisico", "BsContabil", "BsDePara"])
            init_pg(cfg)

        tables = _sqlite_tables(sqlite_conn)
        order = [t for t in ["BsFisico", "BsContabil", "BsDePara"] if t in tables]
        others = [t for t in tables if t not in order and not t.startswith("sqlite_") and t != "conciliados"]

        print(f"[EVS] SQLite tables: {len(tables)}. Migrating: {order + others} + conciliados")

        for t in order + others:
            n = _copy_table(sqlite_conn, pg, t)
            print(f"[EVS] Migrated {t}: {n}")

        nconc = _migrate_conciliados(sqlite_conn, pg)
        if nconc:
            print(f"[EVS] Migrated conciliados: {nconc}")

        with pg.cursor() as cur:
            create_indexes(cur)
        pg.commit()

    sqlite_conn.close()
    print("[EVS] Done.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sqlite", required=True, help="Path to SQLite file (conciliador.db)")
    ap.add_argument("--drop", action="store_true", help="Drop and recreate EVS tables in Postgres")
    args = ap.parse_args()
    migrate(args.sqlite, drop_and_recreate=args.drop)


if __name__ == "__main__":
    main()
