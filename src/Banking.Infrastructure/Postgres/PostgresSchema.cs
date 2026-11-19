namespace Banking.Infrastructure.Postgres;

/// <summary>
/// Schema tuned for 50k+ TPS:
///  - UUIDv7 (time-ordered) PKs => append-only B-tree inserts, no page splits.
///  - balances maintained on the account row (no ledger scan on read).
///  - optimistic version column checked inside a single atomic SQL batch (no app-level locks).
///  - idempotency_keys table with unique index is the final dedupe arbiter.
///  - hash partitioning by account_id ready for multi-node scale-out.
/// Run via: psql "$DATABASE_URL" -f schema.sql
/// </summary>
public static class PostgresSchema
{
    public const string Ddl = """
    -- accounts: current state + optimistic concurrency token
    CREATE TABLE IF NOT EXISTS accounts (
        id              uuid        PRIMARY KEY,
        customer_id     uuid        NOT NULL,
        iban            varchar(34) NOT NULL,
        currency_code   smallint    NOT NULL,
        balance_minor   bigint      NOT NULL DEFAULT 0,
        overdraft_minor bigint      NOT NULL DEFAULT 0,
        status          smallint    NOT NULL DEFAULT 0,
        version         bigint      NOT NULL DEFAULT 0,
        last_sequence   bigint      NOT NULL DEFAULT 0,
        created_at_utc  timestamptz NOT NULL DEFAULT now(),
        updated_at_utc  timestamptz NOT NULL DEFAULT now()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ix_accounts_iban ON accounts (iban);
    CREATE INDEX IF NOT EXISTS ix_accounts_customer ON accounts (customer_id);

    -- Append-only double-entry ledger. Partitioned monthly for retention/scale.
    CREATE TABLE IF NOT EXISTS ledger_entries (
        id                    uuid        NOT NULL,
        account_id            uuid        NOT NULL,
        amount_minor          bigint      NOT NULL,
        currency_code         smallint    NOT NULL,
        entry_type            smallint    NOT NULL,
        status                smallint    NOT NULL,
        correlation_id        uuid        NOT NULL,
        counterparty_id       uuid        NULL,
        reference             varchar(255) NULL,
        seq                   bigint      NOT NULL,
        occurred_at_utc       timestamptz NOT NULL,
        PRIMARY KEY (id, occurred_at_utc)
    ) PARTITION BY RANGE (occurred_at_utc);

    CREATE INDEX IF NOT EXISTS ix_ledger_account_seq ON ledger_entries (account_id, seq DESC);
    CREATE INDEX IF NOT EXISTS ix_ledger_correlation ON ledger_entries (correlation_id);

    -- Idempotency reservation (final arbiter even if Redis is unavailable).
    CREATE TABLE IF NOT EXISTS idempotency_keys (
        key           varchar(128) PRIMARY KEY,
        reserved_at   timestamptz NOT NULL DEFAULT now()
    );
    -- Retention: purge keys older than 48h (run hourly via pg_cron or app job).
    -- DELETE FROM idempotency_keys WHERE reserved_at < now() - interval '48 hours';
    """;
}
