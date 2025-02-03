package benchmark

import (
	"fmt"
	"io"
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

	// Create a copy for testing
	testFile := parquetFile + ".test.parquet"
	if err := copyFile(parquetFile, testFile); err != nil {
		return nil, fmt.Errorf("failed to create test file: %w", err)
	}
	defer os.Remove(testFile) // Clean up test file when done

	results := make([]BenchmarkResult, 0, 4)

	// Benchmark Arrow read
	start := time.Now()
	err := arrowparquet.ReadArrowParquet(testFile)
	results = append(results, BenchmarkResult{
		Implementation: "Arrow",
		Operation:      "Read",
		Duration:       time.Since(start),
		Error:          err,
	})

	// Benchmark Arrow write
	outFile := testFile + ".arrow.parquet"
	start = time.Now()
	err = arrowparquet.WriteArrowParquet(testFile)
	results = append(results, BenchmarkResult{
		Implementation: "Arrow",
		Operation:      "Write",
		Duration:       time.Since(start),
		Error:          err,
	})
	os.Remove(outFile) // Clean up output file

	// Benchmark Parquet-Go read
	start = time.Now()
	_, err = parquetgo.ReadParquetGo(testFile)
	results = append(results, BenchmarkResult{
		Implementation: "ParquetGo",
		Operation:      "Read",
		Duration:       time.Since(start),
		Error:          err,
	})

	// Benchmark Parquet-Go write
	outFile = testFile + ".parquetgo.parquet"
	start = time.Now()
	err = parquetgo.WriteParquetGo(testFile)
	results = append(results, BenchmarkResult{
		Implementation: "ParquetGo",
		Operation:      "Write",
		Duration:       time.Since(start),
		Error:          err,
	})
	os.Remove(outFile) // Clean up output file

	return results, nil
}

// Helper function to copy a file
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	return err
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
