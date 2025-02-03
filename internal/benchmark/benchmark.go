package benchmark

import (
	"fmt"
	"os"
	"time"

	"github.com/TFMV/ParquetBench/internal/arrowparquet"
	"github.com/TFMV/ParquetBench/internal/parquetgo"
)

// BenchmarkResult stores the results of a benchmark run
type BenchmarkResult struct {
	Implementation string
	Operation      string
	Duration       time.Duration
	Error          error
}

// RunBenchmark executes all benchmarks on the provided Parquet file
func RunBenchmark(parquetFile string) ([]BenchmarkResult, error) {
	// Verify file exists and is accessible
	if _, err := os.Stat(parquetFile); err != nil {
		return nil, fmt.Errorf("parquet file not accessible: %w", err)
	}

	results := make([]BenchmarkResult, 0, 2)

	// Benchmark Arrow implementation
	start := time.Now()
	err := arrowparquet.ReadArrowParquet(parquetFile)
	results = append(results, BenchmarkResult{
		Implementation: "Arrow",
		Operation:      "Read",
		Duration:       time.Since(start),
		Error:          err,
	})

	// Benchmark Parquet-Go implementation
	start = time.Now()
	_, err = parquetgo.ReadParquetGo(parquetFile)
	results = append(results, BenchmarkResult{
		Implementation: "ParquetGo",
		Operation:      "Read",
		Duration:       time.Since(start),
		Error:          err,
	})

	return results, nil
}

// PrintResults formats and prints benchmark results
func PrintResults(results []BenchmarkResult) {
	fmt.Println("\n📊 Parquet Benchmark Results")
	fmt.Println("══════════════════════════\n")

	// Find the longest implementation name for padding
	maxLen := 0
	for _, r := range results {
		if len(r.Implementation) > maxLen {
			maxLen = len(r.Implementation)
		}
	}

	for _, result := range results {
		// Status icon
		status := "✅"
		if result.Error != nil {
			status = "❌"
		}

		// Implementation name with padding
		name := fmt.Sprintf("%-*s", maxLen, result.Implementation)

		// Duration with color and formatting
		duration := fmt.Sprintf("%8s", result.Duration.Round(time.Millisecond))

		// Print the result line
		fmt.Printf("%s %s %s: %s",
			status,
			name,
			result.Operation,
			duration,
		)

		if result.Error != nil {
			fmt.Printf(" ⚠️  Error: %v", result.Error)
		}
		fmt.Println()
	}
	fmt.Println()
}
