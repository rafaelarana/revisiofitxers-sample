#!/usr/bin/env python3
"""
Grant the deployed app's Service Principal read access to the Lakebase `revisiofitxers`
state schema. Idempotent. Run AFTER the app is deployed (the SP only exists then) — see
build-plan Phase 3 gate. Called by setup.sh so the whole flow stays one-click.

The pipeline/user owns the `revisiofitxers` schema; the app SP has CAN_CONNECT_AND_CREATE
but no access to schemas it doesn't own, so it needs an explicit GRANT to read state.

Usage:
  python3 scripts/grant_app_access.py --profile <p> --app <app-name> \
      --endpoint projects/<p>/branches/<b>/endpoints/<e> --host <pghost> [--schema revisiofitxers]
"""
from __future__ import annotations

import argparse
import sys

from databricks.sdk import WorkspaceClient


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", default="DEFAULT")
    ap.add_argument("--app", required=True)
    ap.add_argument("--endpoint", required=True)
    ap.add_argument("--host", required=True)
    ap.add_argument("--pg-database", default="databricks_postgres")
    ap.add_argument("--schema", default="revisiofitxers")
    args = ap.parse_args()

    import psycopg

    w = WorkspaceClient(profile=args.profile)
    sp = w.apps.get(name=args.app).service_principal_client_id
    if not sp:
        print(f"Could not resolve service principal for app '{args.app}'", file=sys.stderr)
        return 1
    print(f"App SP client id: {sp}")

    token = w.postgres.generate_database_credential(args.endpoint).token
    user = w.current_user.me().user_name
    dsn = f"host={args.host} dbname={args.pg_database} user={user} password={token} sslmode=require"

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (sp,))
        if not cur.fetchone():
            cur.execute(f'CREATE ROLE "{sp}"')
            print(f"Created Postgres role for SP {sp}")
        for stmt in (
            f'GRANT USAGE ON SCHEMA {args.schema} TO "{sp}"',
            # Read everything; write the expedient-lifecycle tables the app mutates
            # (create expedient, record uploads/corrections). classifications are written
            # by the pipeline, but the app upserts on correction (Phase 5) so grant those too.
            f'GRANT SELECT ON ALL TABLES IN SCHEMA {args.schema} TO "{sp}"',
            f'GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA {args.schema} TO "{sp}"',
            f'GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {args.schema} TO "{sp}"',
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA {args.schema} GRANT SELECT, INSERT, UPDATE ON TABLES TO "{sp}"',
            f'ALTER DEFAULT PRIVILEGES IN SCHEMA {args.schema} GRANT USAGE, SELECT ON SEQUENCES TO "{sp}"',
        ):
            cur.execute(stmt)
    print(f"Granted app SP read+write access on schema '{args.schema}'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
