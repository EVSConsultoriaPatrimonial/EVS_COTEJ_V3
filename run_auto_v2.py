# run_auto_v2_AUTO_ENGINE.py
from __future__ import annotations

from dataclasses import asdict
from typing import List, Optional

# Importa as regras SQLFAST (PostgreSQL automático, fallback SQLite)
from matcher_v2_extended import (
    run_regra_nrbrm_pai,
    run_regra_bem_ant_fis_eq_nrbrm_ctb,
    run_regra_nrbrm_fis_eq_bem_ant_ctb,
    run_regra_exata,
    run_propagacao_incorporados,
)

DEFAULT_SQLITE_DB = "conciliador.db"


def run_automatico_01(db_path: str = DEFAULT_SQLITE_DB) -> List[dict]:
    """
    Automático (01) – SQLFAST:
      - Usa PostgreSQL automaticamente quando disponível.
      - Se não houver PostgreSQL, usa SQLite (db_path).
      - Ordem:
          1) NRBRM físico = NRBRM contábil (apenas INC=0 no contábil)
          2) BEM ANTERIOR (físico) = NRBRM (contábil) (apenas INC=0)
          3) NRBRM (físico) = BEM ANTERIOR (contábil) (apenas INC=0)
          4) SERIE = SERIE (exato) (apenas INC=0, não concilia vazio)
          5) TAG = TAG (exato) (apenas INC=0, não concilia vazio)
          6) Propagação de filhos (INC != 0)
    """
    stats: List[dict] = []
    stats.append(asdict(run_regra_nrbrm_pai(db_path)))
    stats.append(asdict(run_regra_bem_ant_fis_eq_nrbrm_ctb(db_path)))
    stats.append(asdict(run_regra_nrbrm_fis_eq_bem_ant_ctb(db_path)))
    stats.append(asdict(run_regra_exata(db_path, key="SERIE", st="SERIE_FIS_SERIE_CTB", ctb_inc0_only=True)))
    stats.append(asdict(run_regra_exata(db_path, key="TAG", st="TAG_FIS_TAG_CTB", ctb_inc0_only=True)))
    stats.append(asdict(run_propagacao_incorporados(db_path)))
    return stats


def main(db_path: str = DEFAULT_SQLITE_DB) -> List[dict]:
    return run_automatico_01(db_path)


if __name__ == "__main__":
    # Permite rodar direto e ver estatísticas
    import json
    import time

    t0 = time.time()
    out = run_automatico_01(DEFAULT_SQLITE_DB)
    dt = time.time() - t0
    print(json.dumps(out, ensure_ascii=False, indent=2))
    print(f"Tempo total (s): {dt:.2f}")
