using HighThroughputEventIngestion.Api.Background;
using HighThroughputEventIngestion.Api.Endpoints;
using HighThroughputEventIngestion.Core.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Register Core event ingestion & aggregation services
builder.Services.AddEventIngestionCore();

// Register background service for periodic stale device state cleanup
builder.Services.AddHostedService<DeviceCleanupBackgroundService>();

// Register OpenAPI documentation
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

// Map REST API endpoints
app.MapEventEndpoints();

app.Run();

// Exposed for WebApplicationFactory in integration tests
public partial class Program { }
