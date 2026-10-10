using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Tests.Fakes;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HighThroughputEventIngestion.Tests;

public class ApiBehaviorTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly WebApplicationFactory<Program> _factory;
    private readonly FakeTimeProvider _fakeClock;
    private readonly DateTimeOffset _baseTime;

    public ApiBehaviorTests(WebApplicationFactory<Program> factory)
    {
        _baseTime = new DateTimeOffset(2026, 10, 10, 12, 0, 0, TimeSpan.Zero);
        _fakeClock = new FakeTimeProvider(_baseTime);

        _factory = factory.WithWebHostBuilder(builder =>
        {
            builder.ConfigureTestServices(services =>
            {
                services.RemoveAll<TimeProvider>();
                services.AddSingleton<TimeProvider>(_fakeClock);
            });
        });
    }

    private HttpClient CreateClient() => _factory.CreateClient();

    [Fact]
    public async Task PostReadings_ValidPayload_Returns200WithAcceptedCount()
    {
        var client = CreateClient();
        var readings = new[]
        {
            new Reading("sensor-api-1", _baseTime, 21.0),
            new Reading("sensor-api-1", _baseTime.AddSeconds(2), 22.0),
        };

        var response = await client.PostAsJsonAsync("/api/readings", readings);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<IngestionResult>();
        Assert.Equal(2, result.AcceptedCount);
    }

    [Fact]
    public async Task GetAggregate_ValidDevice_Returns200WithCalculatedAggregate()
    {
        var client = CreateClient();
        var readings = new[]
        {
            new Reading("sensor-query-1", _baseTime, 10.0),
            new Reading("sensor-query-1", _baseTime.AddSeconds(5), 20.0),
        };

        await client.PostAsJsonAsync("/api/readings", readings);

        _fakeClock.SetUtcNow(_baseTime.AddSeconds(5));

        var response = await client.GetAsync("/api/devices/sensor-query-1/aggregate");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var aggregate = await response.Content.ReadFromJsonAsync<DeviceAggregate>();
        Assert.Equal("sensor-query-1", aggregate.DeviceId);
        Assert.Equal(2, aggregate.Count);
        Assert.Equal(10.0, aggregate.Min);
        Assert.Equal(20.0, aggregate.Max);
        Assert.Equal(15.0, aggregate.Average);
    }

    [Fact]
    public async Task GetAggregate_UnknownDevice_Returns404NotFound()
    {
        var client = CreateClient();

        var response = await client.GetAsync("/api/devices/non-existent-device/aggregate");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task GetAggregate_DeviceWithAllReadingsExpired_Returns404NotFound()
    {
        var client = CreateClient();
        var readings = new[]
        {
            new Reading("sensor-exp-api", _baseTime, 15.0)
        };

        await client.PostAsJsonAsync("/api/readings", readings);

        // Advance fake clock past 5 minutes
        _fakeClock.SetUtcNow(_baseTime.AddMinutes(6));

        var response = await client.GetAsync("/api/devices/sensor-exp-api/aggregate");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task PostReadings_MalformedJson_Returns400BadRequest()
    {
        var client = CreateClient();
        var content = new StringContent("{ not-valid-json [", Encoding.UTF8, "application/json");

        var response = await client.PostAsync("/api/readings", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task PostReadings_MissingDeviceId_Returns400BadRequest()
    {
        var client = CreateClient();
        var invalidJson = """
        [
            {
                "deviceId": "",
                "timestamp": "2026-10-10T12:00:00Z",
                "value": 25.0
            }
        ]
        """;
        var content = new StringContent(invalidJson, Encoding.UTF8, "application/json");

        var response = await client.PostAsync("/api/readings", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task PostReadings_NonFiniteValue_Returns400BadRequest()
    {
        var client = CreateClient();
        var invalidJson = """
        [
            {
                "deviceId": "sensor-nan",
                "timestamp": "2026-10-10T12:00:00Z",
                "value": "NaN"
            }
        ]
        """;
        var content = new StringContent(invalidJson, Encoding.UTF8, "application/json");

        var response = await client.PostAsync("/api/readings", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task PostReadings_EmptyArray_Returns200WithZeroAccepted()
    {
        var client = CreateClient();
        var content = new StringContent("[]", Encoding.UTF8, "application/json");

        var response = await client.PostAsync("/api/readings", content);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<IngestionResult>();
        Assert.Equal(0, result.AcceptedCount);
    }

    [Fact]
    public async Task PostReadings_EmptyBody_Returns400BadRequest()
    {
        var client = CreateClient();
        var content = new StringContent("", Encoding.UTF8, "application/json");

        var response = await client.PostAsync("/api/readings", content);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task PostReadings_MaxAllowedBatch_Succeeds()
    {
        var client = CreateClient();

        // Create stream with 50,000 readings
        var memoryStream = new MemoryStream();
        await using (var writer = new Utf8JsonWriter(memoryStream))
        {
            writer.WriteStartArray();
            for (int i = 0; i < 50_000; i++)
            {
                writer.WriteStartObject();
                writer.WriteString("deviceId", "sensor-batch-50k");
                writer.WriteString("timestamp", _baseTime.ToString("O"));
                writer.WriteNumber("value", 1.0);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }

        memoryStream.Position = 0;
        using var streamContent = new StreamContent(memoryStream);
        streamContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");

        var response = await client.PostAsync("/api/readings", streamContent);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var result = await response.Content.ReadFromJsonAsync<IngestionResult>();
        Assert.Equal(50_000, result.AcceptedCount);
    }

    [Fact]
    public async Task PostReadings_ExceedingMaxBatchSize_Returns400BadRequest()
    {
        var client = CreateClient();

        // Create stream with 50,001 readings (exceeding limit)
        var memoryStream = new MemoryStream();
        await using (var writer = new Utf8JsonWriter(memoryStream))
        {
            writer.WriteStartArray();
            for (int i = 0; i < 50_001; i++)
            {
                writer.WriteStartObject();
                writer.WriteString("deviceId", "sensor-overflow");
                writer.WriteString("timestamp", _baseTime.ToString("O"));
                writer.WriteNumber("value", 1.0);
                writer.WriteEndObject();
            }
            writer.WriteEndArray();
        }

        memoryStream.Position = 0;
        using var streamContent = new StreamContent(memoryStream);
        streamContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/json");

        var response = await client.PostAsync("/api/readings", streamContent);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }
}
