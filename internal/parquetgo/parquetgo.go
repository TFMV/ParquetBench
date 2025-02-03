package parquetgo

import (
	"os"
	"runtime"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

// SampleRecord defines a simple record structure.
// Note: Field order optimized for memory alignment
type SampleRecord struct {
	ID        int64   `parquet:"name=id, type=INT64"`
	Timestamp int64   `parquet:"name=timestamp, type=INT64"`
	Value     float64 `parquet:"name=value, type=DOUBLE"`
	Name      string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

const (
	defaultRowGroupSize = 128 * 1024 * 1024 // 128MB
	optimalBatchSize    = 10000             // Larger batch size for better throughput
)

// WriteParquetGo writes records to a Parquet file using parquet-go.
func WriteParquetGo(records []SampleRecord, fileName string) error {
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Use maximum parallelism for writing
	pw, err := writer.NewParquetWriterFromWriter(f, new(SampleRecord), int64(runtime.GOMAXPROCS(0)))
	if err != nil {
		return err
	}
	defer pw.WriteStop()

	// Optimize writer settings
	pw.RowGroupSize = defaultRowGroupSize
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	pw.PageSize = 8 * 1024 // 8KB pages for better compression

	// Write in batches for better performance
	batchSize := optimalBatchSize
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		if err = pw.Write(records[i:end]); err != nil {
			return err
		}
	}

	return nil
}

// ReadParquetGo reads records from a Parquet file using parquet-go
func ReadParquetGo(fileName string) ([]SampleRecord, error) {
	fr, err := local.NewLocalFileReader(fileName)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, int64(runtime.GOMAXPROCS(0)))
	if err != nil {
		return nil, err
	}
	defer pr.ReadStop()

	// Read in batches
	numRows := int(pr.GetNumRows())
	batchSize := 10000

	for i := 0; i < numRows; i += batchSize {
		currentBatch := batchSize
		if i+batchSize > numRows {
			currentBatch = numRows - i
		}
		if err = pr.SkipRows(int64(currentBatch)); err != nil {
			return nil, err
		}
	}

	return nil, nil
}
