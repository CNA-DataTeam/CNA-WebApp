from __future__ import annotations

from pathlib import Path
import sys


APP_DIR = Path(__file__).resolve().parents[1]
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

import stocking_agreement_service


def main() -> None:
    templates = stocking_agreement_service.ensure_templates_ready(force_rebuild=True)
    for key, path in templates.items():
        print(f"{key}: {path}")


if __name__ == "__main__":
    main()
