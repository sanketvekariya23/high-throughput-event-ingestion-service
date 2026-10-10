using HighThroughputEventIngestion.Core.Services;

namespace HighThroughputEventIngestion.Api.Background;

/// <summary>
/// Lightweight background service that periodically sweeps and prunes inactive device state.
/// Prevents memory from growing unbounded when devices stop transmitting data.
/// </summary>
public sealed class DeviceCleanupBackgroundService : BackgroundService
{
    private readonly IAggregateService _aggregateService;
    private readonly ILogger<DeviceCleanupBackgroundService> _logger;
    private readonly TimeSpan _cleanupInterval;

    public DeviceCleanupBackgroundService(
        IAggregateService aggregateService,
        ILogger<DeviceCleanupBackgroundService> logger)
        : this(aggregateService, logger, TimeSpan.FromMinutes(1))
    {
    }

    public DeviceCleanupBackgroundService(
        IAggregateService aggregateService,
        ILogger<DeviceCleanupBackgroundService> logger,
        TimeSpan cleanupInterval)
    {
        _aggregateService = aggregateService ?? throw new ArgumentNullException(nameof(aggregateService));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _cleanupInterval = cleanupInterval;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Device cleanup background service started with interval {Interval}.", _cleanupInterval);

        using var timer = new PeriodicTimer(_cleanupInterval);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                {
                    break;
                }

                int pruned = _aggregateService.PruneInactiveDevices();
                if (pruned > 0)
                {
                    _logger.LogInformation("Pruned {PrunedCount} inactive devices from memory.", pruned);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An error occurred during periodic inactive device cleanup.");
            }
        }

        _logger.LogInformation("Device cleanup background service stopped.");
    }
}
