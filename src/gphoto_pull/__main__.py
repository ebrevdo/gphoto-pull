"""Module entrypoint for `python -m gphoto_pull`.

Description:
    Delegates module execution to the typed CLI entrypoint.

Side Effects:
    Runs the CLI and exits with its process-style status code.
"""

from gphoto_pull.cli import main

raise SystemExit(main())
