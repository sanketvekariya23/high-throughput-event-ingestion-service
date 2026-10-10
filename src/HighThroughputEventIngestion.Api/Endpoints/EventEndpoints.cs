using System.Text.Json;
using HighThroughputEventIngestion.Core.Exceptions;
using HighThroughputEventIngestion.Core.Model;
using HighThroughputEventIngestion.Core.Services;
using Microsoft.AspNetCore.Mvc;

namespace HighThroughputEventIngestion.Api.Endpoints;

/// <summary>
/// Route endpoint definitions for event ingestion and device aggregate queries.
/// </summary>
public static class EventEndpoints
{
    public static IEndpointRouteBuilder MapEventEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api");

        group.MapPost("/readings", async (
            HttpContext context,
            IIngestionService ingestionService,
            CancellationToken cancellationToken) =>
        {
            if (context.Request.ContentLength.HasValue && context.Request.ContentLength.Value == 0)
            {
                return Results.BadRequest(new ProblemDetails
                {
                    Status = StatusCodes.Status400BadRequest,
                    Title = "Invalid Request Body",
                    Detail = "The request body cannot be empty. A JSON array of readings is required."
                });
            }

            try
            {
                var result = await ingestionService.IngestAsync(context.Request.Body, cancellationToken);
                return Results.Ok(result);
            }
            catch (JsonException ex)
            {
                return Results.BadRequest(new ProblemDetails
                {
                    Status = StatusCodes.Status400BadRequest,
                    Title = "Malformed JSON",
                    Detail = $"Failed to parse JSON request body: {ex.Message}"
                });
            }
            catch (BatchSizeExceededException ex)
            {
                return Results.BadRequest(new ProblemDetails
                {
                    Status = StatusCodes.Status400BadRequest,
                    Title = "Batch Size Exceeded",
                    Detail = ex.Message
                });
            }
            catch (ReadingValidationException ex)
            {
                return Results.BadRequest(new ProblemDetails
                {
                    Status = StatusCodes.Status400BadRequest,
                    Title = "Validation Error",
                    Detail = ex.Message
                });
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return Results.StatusCode(499);
            }
            catch (Exception)
            {
                return Results.Problem(
                    statusCode: StatusCodes.Status500InternalServerError,
                    title: "Internal Server Error",
                    detail: "An unexpected error occurred while processing the request.");
            }
        })
        .WithName("IngestReadings")
        .WithSummary("Ingests a batch of IoT device readings streaming directly from JSON body.")
        .Produces<IngestionResult>(StatusCodes.Status200OK)
        .Produces<ProblemDetails>(StatusCodes.Status400BadRequest)
        .Produces<ProblemDetails>(StatusCodes.Status500InternalServerError);

        group.MapGet("/devices/{deviceId}/aggregate", (
            string deviceId,
            IAggregateService aggregateService) =>
        {
            if (string.IsNullOrWhiteSpace(deviceId))
            {
                return Results.BadRequest(new ProblemDetails
                {
                    Status = StatusCodes.Status400BadRequest,
                    Title = "Invalid Device ID",
                    Detail = "Device ID must not be empty or whitespace."
                });
            }

            if (aggregateService.TryGetAggregate(deviceId, out var aggregate))
            {
                return Results.Ok(aggregate);
            }

            return Results.NotFound(new ProblemDetails
            {
                Status = StatusCodes.Status404NotFound,
                Title = "Device Not Found or Inactive",
                Detail = $"Device '{deviceId}' does not exist or has no active readings inside the 5-minute rolling window."
            });
        })
        .WithName("GetDeviceAggregate")
        .WithSummary("Retrieves the rolling 5-minute aggregate summary for a device.")
        .Produces<DeviceAggregate>(StatusCodes.Status200OK)
        .Produces<ProblemDetails>(StatusCodes.Status404NotFound)
        .Produces<ProblemDetails>(StatusCodes.Status400BadRequest);

        return app;
    }
}
