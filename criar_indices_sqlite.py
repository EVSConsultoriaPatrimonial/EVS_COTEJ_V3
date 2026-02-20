import sqlite3
from pathlib import Path

DB = Path("conciliador.db")  # mesmo diretório do banco

def criar_indices(db_path: Path):
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    comandos = [
        "CREATE INDEX IF NOT EXISTS idx_fis_nrbrm ON fisico(nrbrm)",
        "CREATE INDEX IF NOT EXISTS idx_fis_serie ON fisico(serie)",
        "CREATE INDEX IF NOT EXISTS idx_fis_modelo ON fisico(modelo)",
        "CREATE INDEX IF NOT EXISTS idx_fis_tag ON fisico(tag)",
        "CREATE INDEX IF NOT EXISTS idx_fis_descricao ON fisico(descricao)",
        "CREATE INDEX IF NOT EXISTS idx_fis_inc ON fisico(inc)",
        "CREATE INDEX IF NOT EXISTS idx_fis_filial ON fisico(filial)",
        "CREATE INDEX IF NOT EXISTS idx_fis_ccusto ON fisico(ccusto)",
        "CREATE INDEX IF NOT EXISTS idx_fis_local ON fisico(local)",

        "CREATE INDEX IF NOT EXISTS idx_ctb_nrbrm ON contabil(nrbrm)",
        "CREATE INDEX IF NOT EXISTS idx_ctb_serie ON contabil(serie)",
        "CREATE INDEX IF NOT EXISTS idx_ctb_modelo ON contabil(modelo)",
        "CREATE INDEX IF NOT EXISTS idx_ctb_tag ON contabil(tag)",
        "CREATE INDEX IF NOT EXISTS idx_ctb_descricao ON contabil(descricao)",
        "CREATE INDEX IF NOT EXISTS idx_ctb_inc ON contabil(inc)",
        "CREATE INDEX IF NOT EXISTS idx_ctb_filial ON contabil(filial)",
        "CREATE INDEX IF NOT EXISTS idx_ctb_ccusto ON contabil(ccusto)",
        "CREATE INDEX IF NOT EXISTS idx_ctb_local ON contabil(local)",

        "CREATE INDEX IF NOT EXISTS idx_depara_fis ON depara(fis_id)",
        "CREATE INDEX IF NOT EXISTS idx_depara_ctb ON depara(ctb_id)",
        "CREATE INDEX IF NOT EXISTS idx_conc_base ON conciliados(BASE)",
    ]

    for sql in comandos:
        cur.execute(sql)

    con.commit()
    con.close()
    print("✅ Índices criados com sucesso.")

if __name__ == "__main__":
    criar_indices(DB)
