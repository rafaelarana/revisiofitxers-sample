#!/usr/bin/env python3
"""
One-time bootstrap: create the Unity Catalog catalog this bundle's schema/volume live in,
if it doesn't already exist. Catalogs aren't a Databricks Asset Bundle resource type (schemas
and volumes are), so this has to run once, before `databricks bundle deploy`.

Usage:
  python3 scripts/ensure_catalog.py --profile <cli-profile> --catalog <name> [--storage-root abfss://... | s3://...]

If the workspace's metastore doesn't have default managed storage enabled, plain creation
fails with "Metastore storage root URL does not exist" — pass --storage-root pointing at a
path under an external location you have CREATE_MANAGED_STORAGE on to fix that.
"""
import argparse
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="DEFAULT")
    p.add_argument("--catalog", default="revisiofitxers_sample")
    p.add_argument("--storage-root", default=None)
    args = p.parse_args()

    w = WorkspaceClient(profile=args.profile)

    try:
        w.catalogs.get(args.catalog)
        print(f"Catalog '{args.catalog}' already exists — nothing to do.")
        return 0
    except NotFound:
        pass

    print(f"Creating catalog '{args.catalog}' (profile={args.profile})...")
    try:
        if args.storage_root:
            w.catalogs.create(name=args.catalog, storage_root=args.storage_root)
        else:
            w.catalogs.create(name=args.catalog)
    except Exception as e:
        print(f"\nFailed to create catalog: {e}", file=sys.stderr)
        print(
            "\nIf this says storage root / metastore default storage is missing, re-run with:\n"
            "  python3 scripts/ensure_catalog.py --profile <profile> --catalog <name> \\\n"
            "    --storage-root abfss://<container>@<account>.dfs.core.windows.net/<path>\n"
            "(or s3://bucket/path on AWS) — pick a path under an external location you can write to.",
            file=sys.stderr,
        )
        return 1

    print(f"Catalog '{args.catalog}' created.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
