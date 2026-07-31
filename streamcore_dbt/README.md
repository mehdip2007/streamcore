# streamcore_dbt

dbt project for StreamCore's warehouse layer — targets **ClickHouse**, not Postgres directly. See
`../CLAUDE.md`'s "dbt project" section for the full picture of why and how.

## Setup

```bash
# From the repo root
pip install -e ".[dbt]"                                      # dbt-core + dbt-clickhouse
cp streamcore_dbt/profiles.example.yml ~/.dbt/profiles.yml    # first time only

# ClickHouse + Postgres must be up
docker compose up -d postgres clickhouse

cd streamcore_dbt
dbt debug   # sanity-check the connection
dbt build   # = dbt run + dbt test, in dependency order
```

`dbt build` will create the `streamcore_raw.events` bridge table in ClickHouse on every run
(idempotent — see `macros/postgres_bridge.sql`), so there's no separate setup step for that.

## Layers

- `models/staging/` — `stg_video_views`, `stg_watch_progress`: 1:1 cleaned windows over the raw events
  bridge, parsed with ClickHouse's `JSONExtract*` functions.
- `models/intermediate/` — `int_watch_sessions`: joins views + progress into full watch sessions.
- `models/marts/` — `mart_content_performance`, `mart_device_quality`: business-ready aggregates.

## Resources

- [dbt docs](https://docs.getdbt.com/docs/introduction)
- [dbt-clickhouse](https://github.com/ClickHouse/dbt-clickhouse)
