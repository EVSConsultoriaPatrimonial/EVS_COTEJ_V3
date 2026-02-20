from __future__ import annotations

import os
import sys

def _add_app_to_syspath() -> None:
    """
    Garante que a pasta 'app' (com seus .py) esteja no sys.path
    tanto em modo normal quanto empacotado pelo PyInstaller.
    """
    # Quando empacotado, _MEIPASS aponta para a pasta temporária do bundle
    base = getattr(sys, "_MEIPASS", os.path.abspath(os.path.dirname(__file__)))

    # No seu build você adicionou --add-data "app:app"
    # Então os arquivos ficam em "<base>/app"
    app_dir = os.path.join(base, "app")

    # Fallback: se não achar, tenta a pasta atual do script (modo dev)
    if not os.path.isdir(app_dir):
        app_dir = os.path.abspath(os.path.dirname(__file__))

    if app_dir not in sys.path:
        sys.path.insert(0, app_dir)

def main() -> None:
    _add_app_to_syspath()
    from interface_inicial_v2 import TelaInicialV2
    app = TelaInicialV2()
    app.mainloop()

if __name__ == "__main__":
    main()
