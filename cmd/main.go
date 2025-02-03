package main

import (
	"fmt"

	"github.com/TFMV/ParquetBench/internal/benchmark"
)

func main() {
	fmt.Println("Running Parquet Benchmarks...")
	results, err := benchmark.RunBenchmark("flights.parquet")
	if err != nil {
		fmt.Printf("Error running benchmarks: %v\n", err)
		return
	}
	fmt.Println("\nBenchmark Report:")
	benchmark.PrintResults(results)
}
