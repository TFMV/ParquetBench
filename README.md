# ParquetBench

ParquetBench is a Go project that benchmarks two Parquet implementations:

- **parquet-go** (from [xitongsys/parquet-go](https://github.com/xitongsys/parquet-go))
- **Arrow’s Parquet implementation** (from [apache/arrow-go](https://github.com/apache/arrow-go))

It generates sample data, writes and reads it using both libraries, and prints a benchmark report showing operation duration and throughput (records per second).

## Project Structure

- `internal/parquetgo/`: parquet-go implementation
- `internal/arrowparquet/`: Arrow’s Parquet implementation
- `cmd/main.go`: main command-line application

## Usage

1. Install dependencies:
   ```bash
   go mod tidy
   ```

2. Run the benchmarks:
   ```bash
   go run cmd/main.go
   ```

## Benchmark Report

The benchmark report will display the following metrics:

- **Library**: The name of the Parquet implementation
- **Operation**: The operation being benchmarked (write or read)
- **Duration**: The time taken to complete the operation
- **Throughput**: The number of records processed per second

## Results

The benchmark results will be displayed in the terminal. For example:

```bash
Running Parquet Benchmarks...

Benchmark Report:

📊 Parquet Benchmark Results
══════════════════════════

✅ Arrow     Read:   1.136s
✅ ParquetGo Read:   3.069s
```

## Author

This project is created by [TFMV](https://github.com/TFMV).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


