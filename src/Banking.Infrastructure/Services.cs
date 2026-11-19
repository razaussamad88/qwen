using Banking.Application;
using Banking.Domain;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Banking.Infrastructure;

/// <summary>Read-side account queries (swap for read-replica connection string in production).</summary>
public sealed class AccountQueryService : IAccountQueryService
{
    private readonly ITransactionStore _store;
    public AccountQueryService(ITransactionStore store) => _store = store;

    public async ValueTask<AccountSnapshot?> GetAccountAsync(AccountId id, CancellationToken ct = default)
    {
        var acc = await _store.LoadAccountAsync(id, ct).ConfigureAwait(false);
        return acc is null ? null : AccountSnapshot.From(acc);
    }
}

/// <summary>DI wiring helpers for the infrastructure layer.</summary>
public static class InfrastructureServiceCollectionExtensions
{
    public static IServiceCollection AddInMemoryBanking(this IServiceCollection services)
    {
        var store = new InMemory.InMemoryTransactionStore();
        // Seed demo accounts so a fresh run is immediately usable.
        Guid customer = Guid.NewGuid();
        var a1 = store.CreateAccount(customer, "DE89370400440532013000", 978, 1_000_000);   // €10,000.00
        var a2 = store.CreateAccount(customer, "GB82WEST12345698765432", 826, 2_500_000);   // £25,000.00
        DemoAccounts.Seed(a1.Id, a2.Id);
        services.AddSingleton(store);
        services.AddSingleton<ITransactionStore>(sp => sp.GetRequiredService<InMemory.InMemoryTransactionStore>());
        services.AddSingleton<IEventSink>(new InMemory.RingBufferEventSink());
        return services;
    }

    public static IServiceCollection AddPostgresBanking(
        this IServiceCollection services,
        Postgres.PostgresOptions pg,
        Redis.RedisOptions? redis = null)
    {
        services.AddSingleton(pg);
        services.AddSingleton(new NpgsqlDataSourceBuilder(pg.ConnectionString));
        services.AddSingleton<Postgres.PostgresTransactionStore>();

        if (redis is not null)
        {
            services.AddSingleton(redis);
            services.AddSingleton<StackExchange.Redis.IConnectionMultiplexer>(sp =>
                StackExchange.Redis.ConnectionMultiplexer.Connect(redis.Endpoints));
            services.AddSingleton<Redis.RedisIdempotencyGuard>();
            services.AddSingleton<ITransactionStore>(sp => sp.GetRequiredService<Redis.RedisBackedTransactionStore>());
        }
        else
        {
            services.AddSingleton<ITransactionStore>(sp => sp.GetRequiredService<Postgres.PostgresTransactionStore>());
        }

        services.AddSingleton<IEventSink>(new InMemory.NullEventSink());
        return services;
    }
}

/// <summary>Holds seeded demo account ids so the API can advertise them.</summary>
public static class DemoAccounts
{
    public static AccountId From { get; private set; }
    public static AccountId To { get; private set; }
    public static bool HasDemo { get; private set; }
    internal static void Seed(AccountId from, AccountId to)
    { From = from; To = to; HasDemo = true; }
}
