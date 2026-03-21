"""
main.py — Calgary Law Tracker v5 entry-point shim
════════════════════════════════════════════════════════════════════════
PURPOSE
  The legacy "Collect & Alert" GitHub Actions workflow calls:
      python main.py          (when MODE="collect", FIRM="")
      python main.py --digest (when MODE="digest")
      python main.py --firm X (when FIRM is set)

  Without this shim, bare `python main.py` hits argparse's else-branch
  (p.print_help()) and exits in < 1 second — no scraping happens at all.

FIX
  Read the MODE / FIRM env vars the old workflow injects and translate
  them into the correct v5 argument before handing off to main_v5.main().

  MODE=collect  (or unset, or any unknown value) → --run deep
  MODE=digest                                    → --digest
  FIRM=<firm_id>                                 → --brief <firm_id>

  The new tracker.yml calls python main_v5.py directly and is unaffected.
════════════════════════════════════════════════════════════════════════
"""
import os, sys

def _inject_v5_args():
    """
    Translate legacy env vars into v5 CLI flags.
    Only injects when the caller supplied zero arguments (i.e. the old
    workflow running bare `python main.py`).
    """
    if len(sys.argv) > 1:
        # Called with explicit args (e.g. python main.py --digest) —
        # already correct, pass straight through.
        return

    mode = os.getenv("MODE", "collect").strip().lower()
    firm = os.getenv("FIRM", "").strip()

    if firm:
        sys.argv.extend(["--brief", firm])
    elif mode == "digest":
        sys.argv.append("--digest")
    else:
        # "collect" / blank / anything else → full deep scrape
        sys.argv.extend(["--run", "deep"])


_inject_v5_args()

from main_v5 import main   # noqa: E402 — import after argv is patched

if __name__ == "__main__":
    main()
