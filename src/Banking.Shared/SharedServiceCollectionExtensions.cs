using Banking.Shared.Messaging;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Banking.Shared;

/// <summary>DI wiring shared by every service that talks to Kafka.</summary>
public static class SharedServiceCollectionExtensions
{
    public static IServiceCollection AddBankingKafka(
        this IServiceCollection services,
        IConfiguration configuration,
        ConsumerGroupOptions? consumerOptions = null)
    {
        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        services.AddSingleton<IKafkaProducerFactory, KafkaProducerFactory>();

        if (consumerOptions is not null)
            services.AddSingleton(Microsoft.Extensions.Options.Options.Create(consumerOptions));

        return services;
    }

    /// <summary>Register a typed consumer hosted service with its handler.</summary>
    public static IServiceCollection AddKafkaConsumer<TValue>(this IServiceCollection services)
        where TValue : class
    {
        services.AddHostedService<KafkaConsumerHostedService<TValue>>();
        return services;
    }
}
