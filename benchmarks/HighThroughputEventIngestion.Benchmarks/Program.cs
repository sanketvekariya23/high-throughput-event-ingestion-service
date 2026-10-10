using BenchmarkDotNet.Running;
using HighThroughputEventIngestion.Benchmarks;

BenchmarkRunner.Run<IngestionBenchmarks>();
