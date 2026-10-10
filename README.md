# High-Throughput Event Ingestion Service

A production-grade, high-throughput, low-latency in-memory event ingestion service built with **.NET 10** and **C# 13**. The service ingests high-volume batches of IoT device telemetry readings (up to 50,000 readings per request) and maintains rolling 5-minute aggregates (`count`, `min`, `max`, `average`) per device with zero external datastores.

---

## 1. Project Overview & Objective

The objective of this service is to ingest continuous streams of IoT device telemetry readings, maintain rolling real-time aggregates over an exact sliding time window of the **last 5 minutes** relative to evaluation time, and answer point-in-time aggregate queries efficiently.

Key design highlights:
- **Zero Full-Request Buffering**: Request bodies are streamed using System.Text.Json async streaming (`IAsyncEnumerable<Reading>`), parsing and processing readings incrementally without materializing 50,000-element collections in memory.
- **Fine-Grained Concurrency**: Operations targeting distinct devices do not contend on the same per-device lock. `ConcurrentDictionary` provides thread-safe device registry operations.
- **Exact Continuous Sliding Window**: Strict half-open interval `(T - 5 minutes, T]`. Readings older than the cutoff expire and are reclaimed immediately.
- **Out-of-Order Resilient**: Handles slightly out-of-order timestamps without sorting entire batches, preserving sorted chronological order inside a memory-efficient circular buffer.
- **Incremental Aggregates**: Maintains `count`, `min`, `max`, and `average` incrementally. Queries execute in \(O(1)\) time without scanning historical arrays.
- **Bounded Memory Lifecycle**: Expired readings are discarded in \(O(1)\), arrays are compacted after burst traffic, and inactive devices that stop transmitting are safely retired and reclaimed by a background maintenance worker.

---

## 2. Solution Architecture & Project Structure

The repository follows a clean, decoupled architecture separating HTTP and protocol concerns from core domain aggregation logic:

```
HighThroughputEventIngestion.slnx
├── src/
│   ├── HighThroughputEventIngestion.Api/          # ASP.NET Core Minimal API layer
│   │   ├── Background/
│   │   │   └── DeviceCleanupBackgroundService.cs  # Periodic background maintenance worker
│   │   ├── Endpoints/
│   │   │   └── EventEndpoints.cs                  # REST endpoint definitions
│   │   └── Program.cs                             # Host setup, middleware, OpenAPI, DI
│   │
│   └── HighThroughputEventIngestion.Core/         # Domain, Windowing, Concurrency, Storage
│       ├── Configuration/
│       │   └── IngestionOptions.cs                # Configurable limits (window, batch size, skew)
│       ├── Exceptions/
│       │   ├── BatchSizeExceededException.cs      # Batch size threshold enforcement
│       │   └── ReadingValidationException.cs      # Domain validation failures
│       ├── Extensions/
│       │   └── ServiceCollectionExtensions.cs     # Core DI wireup
│       ├── Model/
│       │   ├── DeviceAggregate.cs                 # Query response model
│       │   ├── IngestionResult.cs                 # Ingestion response model
│       │   └── Reading.cs                         # Input DTO record struct
│       ├── Services/
│       │   ├── AggregateService.cs                # Aggregate query facade
│       │   ├── IAggregateService.cs
│       │   ├── IngestionService.cs                # Streaming ingestion processor
│       │   └── IIngestionService.cs
│       ├── Storage/
│       │   ├── ConcurrentDeviceStore.cs           # Concurrent device registry
│       │   ├── DeviceState.cs                     # Per-device circular buffer & window engine
│       │   ├── IDeviceStore.cs
│       │   └── TimestampedReading.cs              # Compact 16-byte unmanaged value type
│       └── Validation/
│           └── ReadingValidator.cs                # High-performance payload validation
│
├── tests/
│   └── HighThroughputEventIngestion.Tests/        # Comprehensive unit & integration tests
│       ├── Fakes/
│       │   └── FakeTimeProvider.cs                # Deterministic virtual clock
│       ├── AggregateCorrectnessTests.cs           # Math, min, max, average accuracy
│       ├── ApiBehaviorTests.cs                    # WebApplicationFactory HTTP tests
│       ├── BasicIngestionTests.cs                 # Single, empty, multiple device batches
│       ├── ConcurrencyTests.cs                    # Race conditions, lost updates, tearing
│       ├── MemoryAndCleanupTests.cs               # Expiration, compaction, device retirement
│       ├── OutOfOrderDataTests.cs                 # Jitter, out-of-order, late readings
│       └── SlidingWindowTests.cs                  # Exact boundary & continuous advancement
│
└── benchmarks/
    └── HighThroughputEventIngestion.Benchmarks/   # BenchmarkDotNet performance suite
        ├── IngestionBenchmarks.cs                 # Microbenchmarks (100 to 50k readings)
        └── Program.cs                             # Runner entry point
```

---

## 3. Prerequisites

- **.NET 10 SDK** (v10.0.300 or later)
- Windows, Linux, or macOS

---

## 4. Build, Test & Run Instructions

### Restore Dependencies
```bash
dotnet restore
```

### Build Solution (Debug)
```bash
dotnet build
```

### Build Solution (Release)
```bash
dotnet build -c Release
```

### Run All Automated Tests
```bash
dotnet test
```

### Run API Service
```bash
dotnet run --project src/HighThroughputEventIngestion.Api/HighThroughputEventIngestion.Api.csproj
```
The API starts on `https://localhost:7147` / `http://localhost:5246` (see `Properties/launchSettings.json`).

---

## 5. REST API Endpoints

### 1. Ingest Telemetry Readings
- **Method**: `POST`
- **Route**: `/api/readings`
- **Content-Type**: `application/json`
- **Max Batch**: 50,000 readings per request
- **Response**: `200 OK` on success, `400 Bad Request` on malformed JSON, batch size overflow, or invalid readings.

#### Example Request
```http
POST /api/readings HTTP/1.1
Host: localhost:5246
Content-Type: application/json

[
  {
    "deviceId": "sensor-0042",
    "timestamp": "2026-10-10T12:00:01Z",
    "value": 21.0
  },
  {
    "deviceId": "sensor-0042",
    "timestamp": "2026-10-10T12:00:03Z",
    "value": 24.5
  },
  {
    "deviceId": "sensor-0099",
    "timestamp": "2026-10-10T12:00:02Z",
    "value": 18.2
  }
]
```

#### Example Response (200 OK)
```json
{
  "acceptedCount": 3
}
```

---

### 2. Query Device Rolling Aggregate
- **Method**: `GET`
- **Route**: `/api/devices/{deviceId}/aggregate`
- **Response**:
  - `200 OK` if the device exists and has active readings inside the 5-minute rolling window.
  - `404 Not Found` if the device does not exist or all readings have expired.
  - `400 Bad Request` if `deviceId` is invalid or empty.

#### Example Request
```http
GET /api/devices/sensor-0042/aggregate HTTP/1.1
Host: localhost:5246
```

#### Example Response (200 OK)
```json
{
  "deviceId": "sensor-0042",
  "count": 2,
  "min": 21.0,
  "max": 24.5,
  "average": 22.75
}
```

#### Example Response (404 Not Found)
```json
{
  "type": "https://tools.ietf.org/html/rfc9110#section-15.5.5",
  "title": "Device Not Found or Inactive",
  "status": 404,
  "detail": "Device 'sensor-0042' does not exist or has no active readings inside the 5-minute rolling window."
}
```

---

## 6. Sliding Window & Time Semantics

### Mathematical Definition
For evaluation time \(T\), the active sliding window is defined as the **half-open interval**:
$$\text{Active Window} = (T - 5\text{ minutes}, T]$$

- **Lower Boundary (\(T - 5\text{ minutes}\))**:
  - A reading with \(\text{timestamp} \le T - 5\text{ minutes}\) is **EXPIRED** and does not contribute to `count`, `min`, `max`, or `average`.
  - A reading with \(\text{timestamp} > T - 5\text{ minutes}\) is **ACTIVE**.
- **Upper Boundary (\(T\))**:
  - A reading with \(\text{timestamp} \le T\) is **ACTIVE**.
  - Any future timestamped reading (\(T < \text{timestamp} \le T + \text{tolerance}\)) entering due to minor clock skew is accepted and stored chronologically, but only becomes active once the evaluation clock advances to its timestamp.
- **Continuous Advancement**:
  - The window is evaluated dynamically against exact timestamp comparisons using `DateTimeOffset` and `TimeProvider`. There are **no fixed time buckets** (e.g., 1-minute tumbling buckets). The active window is defined continuously by the interval `(T - 5 minutes, T]`.

### Time Semantics & Policies
- **Ingestion Batch Reference Time**: Captured once per batch arrival (`now = _timeProvider.GetUtcNow()`). This provides a single, consistent snapshot baseline for validating and windowing the entire batch, eliminating up to 50,000 system clock calls per request.
- **Query Reference Time**: Captured dynamically at the moment the query is executed (`now = _timeProvider.GetUtcNow()`), ensuring the caller receives the active window evaluated by exact timestamp comparison against the defined interval `(T - 5 minutes, T]`.
- **Clock Skew & Future Timestamps**: A configurable `ClockSkewFutureTolerance` (default: 1 minute) accommodates realistic IoT device clock drift. Readings with timestamps exceeding `now + tolerance` are rejected with HTTP 400 Bad Request (`ReadingValidationException`).
- **Late Readings**: Readings arriving with timestamps already \(\le now - 5\text{ minutes}\) are safely dropped from the active buffer, preserving aggregate integrity.
- **Partial Batch Ingestion Semantics on Threshold Overflow**: When streaming a request exceeding the 50,000 limit (e.g., 50,001 readings), the service processes readings incrementally. Upon encountering reading 50,001, a `BatchSizeExceededException` is thrown, halting further streaming and returning HTTP 400 Bad Request. Any readings processed prior to reaching the limit remain in their respective device states (streaming forward semantics).

---

## 7. Out-of-Order Handling & Algorithmic Complexity

### Complexity Analysis of `InsertReadingInternal`
The internal buffer is a circular array (`TimestampedReading[]`) designed for cache locality; stored `TimestampedReading` entries are value types and do not require a separate heap object allocation per reading. The overall ingestion pipeline still performs allocations, as demonstrated by BenchmarkDotNet:
- **Best Case / In-Order Arrival**: \(O(1)\). Readings arriving chronologically are appended directly to the head of the circular buffer.
- **Expected Case / Moderate Jitter**: \(O(K)\), where \(K\) is the displacement distance from the latest reading. Real-world IoT network jitter typically involves \(K \le 3\) positions. Shifting \(K\) elements in a contiguous unmanaged array requires only sequential memory displacement.
- **Worst Case**: \(O(N)\) for a pathological stream where all \(N\) readings for a single device arrive in strictly reverse chronological order. Because \(N\) is bounded by the 5-minute window and device capacity, contiguous memory block shifts via `Array.Copy` or pointer spans execute efficiently.
- **Design Justification**:
  - `LinkedList<T>` would allocate a 40-byte `LinkedListNode<T>` object per reading (heap churn per request).
  - `SortedSet<T>` or `SortedDictionary<K, V>` (Red-Black Trees) allocate tree nodes per entry and require \(O(\log N)\) pointer-chasing traversals that defeat CPU L1/L2 cache prefetching.
  - The contiguous circular buffer ensures stored `TimestampedReading` entries are value types and do not require a separate heap object allocation per reading. The overall ingestion pipeline still performs allocations, as demonstrated by BenchmarkDotNet.

---

## 8. Concurrency & Thread-Safety Approach

1. **Partitioned Per-Device Locking**:
   - Devices are registered in a thread-safe `ConcurrentDictionary<string, DeviceState>`.
   - Each `DeviceState` maintains its own dedicated synchronization lock (`_gate`).
   - Operations targeting distinct devices do not contend on the same per-device lock. `ConcurrentDictionary` provides thread-safe device registry operations.
   - Concurrent batches updating the same device synchronize via the per-device lock. Critical sections are brief, executing strictly in-memory pointer and arithmetic operations with zero I/O.
2. **Consistent Atomic Snapshots**:
   - Aggregate queries enter the per-device lock, expire stale readings against the evaluation timestamp, and take an atomic snapshot of `count`, `min`, `max`, and `average`.
   - Readers can never observe torn states (e.g., `min > max` or mismatched `count` vs `average`).
3. **Safe Device Retirement Without Global Locks**:
   - Stale devices are pruned using atomic dictionary removals (`TryRemove(KeyValuePair<string, DeviceState>)`) under the device's lock, ensuring concurrent ingestion racing with a cleanup sweep detects retirement and safely acquires a fresh instance without lost updates.

---

## 9. Memory Management & Bounded State

1. **Compact Unmanaged Representation**:
   - Each active reading is stored as a 16-byte `readonly struct TimestampedReading(long Ticks, double Value)`.
   - Stored `TimestampedReading` entries are value types and do not require a separate heap object allocation per reading. The overall ingestion pipeline still performs allocations, as demonstrated by BenchmarkDotNet.
2. **Immediate Expiration**:
   - Ingestion and query paths expire outdated readings immediately in \(O(1)\) amortized time by advancing the ring buffer head pointer.
3. **Buffer Compaction**:
   - If a device receives a burst of readings causing its buffer to grow and subsequently drains, the internal buffer is trimmed back to bounded size once active count drops below 25% of capacity.
4. **Stale Device Retirement**:
   - The `DeviceCleanupBackgroundService` runs periodically (every 1 minute) to retire devices that have no active readings and have remained idle for over 10 minutes, ensuring memory does not grow unbounded if devices cease sending data.

---

## 10. Performance & Allocation Strategy

- **JSON Streaming**: Deserialization uses `JsonSerializer.DeserializeAsyncEnumerable<Reading>`, streaming readings item by item directly from the HTTP request stream into the aggregation engine. Large 50,000-element collections (`List<Reading>`) are never materialized in memory.
- **Zero LINQ on Ingestion Path**: Ring buffer manipulations, insertions, and incremental accumulations use plain loop primitives.
- **Incremental Aggregates**:
  - `Count` is updated via direct increment/decrement.
  - `Average` is maintained via `sum / count`, resetting sum to 0.0 when empty to prevent floating-point rounding drift.
  - `Min` and `Max` track active frequencies (`_minCount`, `_maxCount`). When an expiring reading was not the minimum or maximum, min/max are maintained in \(O(1)\) with zero scans. Only when the last instance of the minimum/maximum expires is a linear scan over the active buffer performed.

---

## 11. Benchmark Instructions & Empirical Results

The benchmark suite is located in `benchmarks/HighThroughputEventIngestion.Benchmarks` and utilizes **BenchmarkDotNet v0.15.8**.

### Running the Benchmarks
To execute the benchmarks under Release optimizations:
```bash
dotnet run -c Release --project benchmarks/HighThroughputEventIngestion.Benchmarks/HighThroughputEventIngestion.Benchmarks.csproj
```

### Empirical Benchmark Results
The following data represents empirical measurements observed on the benchmark test environment (Intel Core i5-6200U CPU @ 2.30 GHz, 2 physical cores, 4 logical threads, Windows 10, .NET 10.0.10) under BenchmarkDotNet v0.15.8. These demonstrate relative allocation and throughput characteristics on this host, not guaranteed production capacity:

```
| Method                                     | BatchSize | Mean          | Error       | StdDev        | Median        | Gen0      | Gen1     | Gen2     | Allocated  |
|------------------------------------------- |---------- |--------------:|------------:|--------------:|--------------:|----------:|---------:|---------:|-----------:|
| IngestBatch_InOrder                        | 100       |     17.510 us |   0.3475 us |     0.5925 us |     17.387 us |    4.2725 |        - |        - |    6.55 KB |
| IngestBatch_InOrder                        | 1000      |    181.761 us |   3.5284 us |     3.7754 us |    181.652 us |   36.8652 |        - |        - |   56.44 KB |
| IngestBatch_InOrder                        | 10000     |  1,876.104 us |  35.2536 us |    37.7203 us |  1,871.493 us |  363.2813 |        - |        - |  557.54 KB |
| IngestBatch_InOrder                        | 50000     | 10,022.463 us | 196.4718 us |   333.6268 us |  9,957.514 us | 1812.5000 |        - |        - | 2788.16 KB |
| IngestBatch_OutOfOrder_Jitter              | 50000     |  8,851.520 us | 175.7684 us |   251.7225 us |  8,784.887 us | 1812.5000 |        - |        - | 2787.95 KB |
| IngestStreamAsync_Utf8Json                 | 50000     | 62,692.651 us | 988.6675 us | 1,057.8741 us | 62,569.214 us | 1000.0000 | 500.0000 | 250.0000 | 7617.54 KB |
```

### Observations from Benchmark Environment
These measurements reflect results observed in the local benchmark environment (not guaranteed production capacity):
1. **Raw Ingestion**: Ingestion and aggregate calculation for 50,000 in-order readings completed in ~10.02 ms on the benchmark host.
2. **Out-of-Order Jitter**: Ingestion of 50,000 readings with simulated arrival jitter completed in ~8.85 ms on the benchmark host.
3. **End-to-End JSON Streaming**: Deserialization and ingestion of 50,000 readings via UTF-8 stream completed in ~62.69 ms on the benchmark host.
4. **Allocation Footprint**: As measured by BenchmarkDotNet, managed allocations averaged ~55 bytes per reading for raw ingestion and ~152 bytes per reading for streaming JSON.

---

## 11. Assumptions, Limitations & Out-of-Scope Items

### Assumptions
- Device readings have ISO-8601 UTC timestamps.
- Out-of-order readings arrive with moderate temporal jitter (within the active window).
- Network disconnections trigger standard cancellation tokens.

### Limitations
- **Single Process In-Memory State**: State is non-durable across process restarts.
- **Hardware Bound**: Capacity is governed by available system RAM and CPU core count.

### Out-of-Scope Items
- Authentication & Authorization (OAuth2, API keys)
- Rate Limiting / WAF
- Persistent storage / databases (PostgreSQL, Redis, ClickHouse)
- Horizontal clustering / distributed consensus
