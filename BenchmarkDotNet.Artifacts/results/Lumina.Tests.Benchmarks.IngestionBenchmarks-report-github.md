```

BenchmarkDotNet v0.15.8, Windows 10 (10.0.19045.6456/22H2/2022Update)
Intel Core i7-6700 CPU 3.40GHz (Max: 3.41GHz) (Skylake), 1 CPU, 8 logical and 4 physical cores
.NET SDK 10.0.103
  [Host]     : .NET 10.0.3 (10.0.3, 10.0.326.7603), X64 RyuJIT x86-64-v3
  DefaultJob : .NET 10.0.3 (10.0.3, 10.0.326.7603), X64 RyuJIT x86-64-v3


```
| Method                      | BatchSize | Mean         | Error      | StdDev     | P95          | MB/s  | Gen0      | Gen1     | Allocated  |
|---------------------------- |---------- |-------------:|-----------:|-----------:|-------------:|------:|----------:|---------:|-----------:|
| **&#39;WAL WriteBatchAsync&#39;**       | **1000**      |     **2.213 ms** |  **0.0271 ms** |  **0.0212 ms** |     **2.241 ms** |  **97,4** |   **74.2188** |  **23.4375** |  **328.64 KB** |
| &#39;WAL Sequential WriteAsync&#39; | 1000      |   226.622 ms |  1.3224 ms |  1.1043 ms |   228.076 ms |   1,0 |         - |        - |  601.75 KB |
| **&#39;WAL WriteBatchAsync&#39;**       | **10000**     |    **19.477 ms** |  **0.2609 ms** |  **0.2440 ms** |    **19.850 ms** | **110,7** |  **593.7500** | **375.0000** | **3281.78 KB** |
| &#39;WAL Sequential WriteAsync&#39; | 10000     | 2,282.325 ms | 16.1673 ms | 15.1229 ms | 2,311.018 ms |   0,9 | 1000.0000 |        - | 6015.81 KB |
