"""
main.py — compatibility shim for Calgary Law Tracker v5
────────────────────────────────────────────────────────
The GitHub Actions workflow "Collect & Alert" (legacy) calls:
    python main.py
This file was previously written against the v3/v4 OOP database
interface (`from database.db import Database`) which no longer
exists — db.py was refactored to a functional API in v5.

FIX: this shim simply delegates to main_v5.py so the old workflow
entry point continues to work, and the new tracker.yml workflow
(which calls python main_v5.py directly) also works unchanged.
"""
from main_v5 import main

if __name__ == "__main__":
    main()
