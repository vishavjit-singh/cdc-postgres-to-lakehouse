"""Prove the lake matches the source: row counts, per-key differences and an order-independent checksum."""
from __future__ import annotations

from .state import canonical, table_checksum


def reconcile(source: dict[str, list[dict]], lake: dict[str, list[dict]],
              primary_keys: dict[str, tuple[str, ...]]) -> list[dict]:
    """``source`` and ``lake`` map table → live rows (lake rows without soft-deleted keys)."""
    report = []
    for table, pk in primary_keys.items():
        src_rows, lake_rows = source.get(table, []), lake.get(table, [])
        columns = sorted({c for r in src_rows for c in r} | {c for r in lake_rows for c in r if not c.startswith("_")})
        src = {tuple(r[k] for k in pk): r for r in src_rows}
        dst = {tuple(canonical(r[k]) for k in pk): r for r in lake_rows}
        missing = sorted(set(src) - set(dst))
        extra = sorted(set(dst) - set(src))
        mismatched = sorted(k for k in set(src) & set(dst)
                            if any(canonical(src[k].get(c)) != canonical(dst[k].get(c)) for c in columns))
        cs_src = table_checksum(src_rows, columns, pk)
        cs_lake = table_checksum([{c: r.get(c) for c in columns} for r in lake_rows], columns, pk)
        report.append({"table": table, "source_rows": len(src), "lake_rows": len(dst), "missing": len(missing),
                       "extra": len(extra), "mismatched": len(mismatched), "source_checksum": cs_src,
                       "lake_checksum": cs_lake, "match": cs_src == cs_lake and not (missing or extra or mismatched),
                       "examples": [list(k) for k in (missing + extra + mismatched)[:5]]})
    return report


def source_from_postgres(dsn: str, tables: dict[str, tuple[str, ...]]) -> dict[str, list[dict]]:
    """Read the live source tables (docker compose stack). Values are cast the way Debezium serialises them:
    numerics as strings (``decimal.handling.mode=string``) and timestamptz as ISO-8601 UTC strings."""
    import psycopg

    out = {}
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for table in tables:
            cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = 'public' AND table_name = %s ORDER BY ordinal_position", (table,))
            cols = cur.fetchall()
            select = ", ".join(
                f"to_char({c} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS {c}"
                if t.startswith("timestamp") else f"{c}::text AS {c}" if t == "numeric" else c for c, t in cols)
            cur.execute(f"SELECT {select} FROM public.{table}")
            names = [d.name for d in cur.description]
            out[table] = [dict(zip(names, row)) for row in cur.fetchall()]
    return out
