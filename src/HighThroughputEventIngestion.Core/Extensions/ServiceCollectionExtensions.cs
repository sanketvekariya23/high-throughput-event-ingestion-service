using HighThroughputEventIngestion.Core.Configuration;
using HighThroughputEventIngestion.Core.Services;
using HighThroughputEventIngestion.Core.Services.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HighThroughputEventIngestion.Core.Extensions;

/// <summary>
/// Service collection extension methods for registering Core event ingestion services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers all necessary core event ingestion and aggregation services.
    /// </summary>
    public static IServiceCollection AddEventIngestionCore(
        this IServiceCollection services,
        Action<IngestionOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var options = new IngestionOptions();
        configureOptions?.Invoke(options);
        services.TryAddSingleton(options);

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IDeviceStore, ConcurrentDeviceStore>();
        services.TryAddSingleton<IIngestionService, IngestionService>();
        services.TryAddSingleton<IAggregateService, AggregateService>();

        return services;
    }
}
