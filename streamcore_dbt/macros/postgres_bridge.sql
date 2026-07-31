{#
    dbt now targets ClickHouse (see profiles.yml), but the Kafka consumer
    and the Spark job are unchanged — they still write to Postgres
    (streamcore_raw.events, streamcore_aggregated.*). Rather than build a
    second ingestion pipeline into ClickHouse, these on-run-start hooks
    (see dbt_project.yml) create a table INSIDE ClickHouse that uses
    ClickHouse's PostgreSQL table engine: a live federated view of the
    real Postgres table, not a copy. dbt's `source('streamcore_raw',
    'events')` then reads through this bridge exactly like a normal table.

    Two macros instead of one because dbt executes each on-run-start
    entry as a single statement — CREATE DATABASE and CREATE TABLE can't
    be combined into one.
#}

{% macro create_raw_events_database() %}
    CREATE DATABASE IF NOT EXISTS streamcore_raw
{% endmacro %}

{% macro create_raw_events_bridge_table() %}
    {#
        'postgres' is the docker-compose service hostname. This is
        resolved by the ClickHouse SERVER (wherever it runs inside the
        Docker network), not by whatever machine happens to invoke
        `dbt build` — so this stays 'postgres:5432' whether dbt is run
        from a developer's laptop or from inside the airflow container.
    #}
    CREATE TABLE IF NOT EXISTS streamcore_raw.events
    (
        id Int64,
        event_id String,
        event_type String,
        event_timestamp DateTime64(6, 'UTC'),
        payload String,
        ingested_at DateTime64(6, 'UTC'),
        kafka_topic Nullable(String),
        kafka_partition Nullable(Int32),
        kafka_offset Nullable(Int64)
    )
    ENGINE = PostgreSQL(
        'postgres:5432',
        '{{ env_var("POSTGRES_DB", "streamcore") }}',
        'events',
        '{{ env_var("POSTGRES_USER", "streamcore") }}',
        '{{ env_var("POSTGRES_PASSWORD", "change_me_locally") }}',
        'streamcore_raw'
    )
{% endmacro %}
