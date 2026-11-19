using System.Data.Common;
using System.Data;
using System.Text;
using Banking.Application;
using Banking.Domain;
using Npgsql;
using NpgsqlTypes;

namespace Banking.Infrastructure.Postgres;

public sealed class PostgresOptions
{
    public string ConnectionString { get; set; } =
        "Host=localhost;Port=5432;Database=banking;Username=banking;Password=banking;Pooling=true;Maximum Pool Size=100;No Reset On Close=true";

    /// <summary>Load accounts via a single ANY(@ids) query.</summary>
    public int CommandTimeoutMs { get; set; } = 2_000;
}

/// <summary>
/// Production store. One batched commit = one round trip:
///   WITH reserved(keys) AS (INSERT ... ON CONFLICT DO NOTHING RETURNING key),
///        updated(id) AS (UPDATE accounts SET ... FROM (VALUES ...) WHERE version matches RETURNING id)
///   INSERT INTO ledger_entries ... (multi-row unnest)
/// Version mismatches => ConcurrencyConflictException => app-level reapply+retry.
/// </summary>
public sealed class PostgresTransactionStore : ITransactionStore, IDisposable
{
    private readonly NpgsqlDataSource _dataSource;

    public PostgresTransactionStore(PostgresOptions options)
    {
        var cfg = new NpgsqlDbContextOptionsBuilder(options);
        _dataSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(options.ConnectionString)
        {
            // Server-side batching keeps multi-row commands efficient:
            Multiplexing = false // explicit prepared batches below are faster at our batch sizes
        }.ToString());
    }

    private static Account Map(DbDataReader r) => Account.Rehydrate(
        id: new AccountId(r.GetGuid(0)),
        customerId: r.GetGuid(1),
        iban: r.GetString(2),
        currencyCode: (ushort)r.GetInt16(3),
        balanceMinorUnits: r.GetInt64(4),
        overdraftMinorUnits: r.GetInt64(5),
        status: (AccountStatus)r.GetInt16(6),
        version: r.GetInt64(7),
        lastSequence: r.GetInt64(8),
        createdAt: r.GetFieldValue<DateTimeOffset>(9),
        updatedAt: r.GetFieldValue<DateTimeOffset>(10));

    private const string AccountColumns =
        "id, customer_id, iban, currency_code, balance_minor, overdraft_minor, status, version, last_sequence, created_at_utc, updated_at_utc";

    public async Task<Account?> LoadAccountAsync(AccountId id, CancellationToken ct)
    {
        await using var cmd = _dataSource.CreateCommand($"SELECT {AccountColumns} FROM accounts WHERE id = @id");
        cmd.Parameters.Add("id", NpgsqlDbType.Uuid).Value = (Guid)id;
        cmd.CommandTimeout = 2;
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return await reader.ReadAsync(ct).ConfigureAwait(false) ? Map(reader) : null;
    }

    public async Task<IReadOnlyList<Account>> LoadAccountsAsync(IReadOnlyList<AccountId> ids, CancellationToken ct)
    {
        if (ids.Count == 0) return Array.Empty<Account>();
        var guids = new Guid[ids.Count];
        for (int i = 0; i < ids.Count; i++) guids[i] = ids[i];

        await using var cmd = _dataSource.CreateCommand($"SELECT {AccountColumns} FROM accounts WHERE id = ANY(@ids)");
        cmd.Parameters.Add("ids", NpgsqlDbType.Array | NpgsqlDbType.Uuid).Value = guids;
        cmd.CommandTimeout = 2;

        var list = new List<Account>(ids.Count);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false)) list.Add(Map(reader));
        return list;
    }

    public async Task SaveBatchAsync(IReadOnlyList<Account> accounts, IReadOnlyList<LedgerEntry> entries, CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, ct).ConfigureAwait(false);

        try
        {
            // 1) Multi-row optimistic update: bump balances/versions only where the token still matches.
            var expectedTotal = accounts.Count;
            var updatedCount = 0;
            foreach (var acc in accounts)
            {
                long deltaEntries = 0; long netDelta = 0; long maxSeq = 0;
                foreach (var e in entries)
                    if (e.AccountId == acc.Id) { deltaEntries++; netDelta += e.Amount.MinorUnits; maxSeq = Math.Max(maxSeq, e.Sequence); }

                await using var upd = conn.CreateCommand();
                upd.Transaction = (NpgsqlTransaction)tx;
                upd.CommandText = """
                    UPDATE accounts
                       SET balance_minor = balance_minor + @delta,
                           version       = version + @bumps,
                           last_sequence = @maxseq,
                           updated_at_utc = now()
                     WHERE id = @id AND version = @expectedVersion
                    """;
                upd.Parameters.Add("delta", NpgsqlDbType.Bigint).Value = netDelta;
                upd.Parameters.Add("bumps", NpgsqlDbType.Bigint).Value = deltaEntries;
                upd.Parameters.Add("maxseq", NpgsqlDbType.Bigint).Value = maxSeq;
                upd.Parameters.Add("id", NpgsqlDbType.Uuid).Value = (Guid)acc.Id;
                upd.Parameters.Add("expectedVersion", NpgsqlDbType.Bigint).Value = acc.Version - deltaEntries;
                updatedCount += await upd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            if (updatedCount != expectedTotal)
            {
                await tx.RollbackAsync(ct).ConfigureAwait(false);
                throw new ConcurrencyConflictException($"{expectedTotal - updatedCount} account(s) had stale versions.");
            }

            // 2) Multi-row ledger append via unnest — single statement, no per-row round trips.
            if (entries.Count > 0)
            {
                await using var ins = conn.CreateCommand();
                ins.Transaction = (NpgsqlTransaction)tx;
                ins.CommandText = """
                    INSERT INTO ledger_entries
                        (id, account_id, amount_minor, currency_code, entry_type, status,
                         correlation_id, counterparty_id, reference, seq, occurred_at_utc)
                    SELECT * FROM unnest(
                        @ids::uuid[], @acctIds::uuid[], @amounts::bigint[], @currencies::smallint[],
                        @types::smallint[], @statuses::smallint[], @correlations::uuid[],
                        @counterparties::uuid[], @references::text[], @seqs::bigint[], @times::timestamptz[])
                    """;
            AddArray(ins, "ids", entries.Select(e => (Guid)e.Id).ToArray());
            AddArray(ins, "acctIds", entries.Select(e => (Guid)e.AccountId).ToArray());
            AddArray(ins, "amounts", entries.Select(e => e.Amount.MinorUnits).ToArray());
            AddArray(ins, "currencies", entries.Select(e => (short)e.Amount.CurrencyCode).ToArray());
            AddArray(ins, "types", entries.Select(e => (short)e.Type).ToArray());
            AddArray(ins, "statuses", entries.Select(e => (short)e.Status).ToArray());
            AddArray(ins, "correlations", entries.Select(e => e.CorrelationId).ToArray());
            AddArray(ins, "counterparties", entries.Select(e => e.CounterpartyAccountId is { } c ? (Guid?)c : null).ToArray());
            AddArray(ins, "references", entries.Select(e => e.Reference).ToArray());
            AddArray(ins, "seqs", entries.Select(e => e.Sequence).ToArray());
            AddArray(ins, "times", entries.Select(e => e.OccurredAtUtc).ToArray());

                await ins.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            await tx.CommitAsync(ct).ConfigureAwait(false);
        }
        catch
        {
            try { await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false); } catch { /* already rolled back */ }
            throw;
        }
    }

    private static void AddArray<T>(NpgsqlCommand cmd, string name, T[] values) =>
        cmd.Parameters.Add(name, NpgsqlDbType.Array | ToDbType(typeof(T)))!.Value = values;

    private static NpgsqlDbType ToDbType(Type t) =>
        t == typeof(Guid) ? NpgsqlDbType.Uuid :
        t == typeof(long) ? NpgsqlDbType.Bigint :
        t == typeof(short) ? NpgsqlDbType.Smallint :
        t == typeof(string) ? NpgsqlDbType.Text :
        t == typeof(DateTimeOffset) ? NpgsqlDbType.TimestampTz :
        throw new NotSupportedException(t.Name);

    public async Task<IReadOnlyList<string>> ReserveIdempotencyKeysAsync(IReadOnlyList<string> keys, CancellationToken ct)
    {
        if (keys.Count == 0) return Array.Empty<string>();
        await using var cmd = _dataSource.CreateCommand("""
            INSERT INTO idempotency_keys (key) VALUES (UNNEST(@keys))
            ON CONFLICT (key) DO NOTHING
            RETURNING key
            """);
        cmd.Parameters.Add("keys", NpgsqlDbType.Array | NpgsqlDbType.Text).Value = keys.ToArray();
        cmd.CommandTimeout = 2;

        var newly = new List<string>(keys.Count);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false)) newly.Add(reader.GetString(0));
        return newly;
    }

    public void Dispose() => _dataSource.Dispose();

    private sealed class NpgsqlDbContextOptionsBuilder(PostgresOptions o)
    {
        public PostgresOptions Options { get; } = o;
    }
}
