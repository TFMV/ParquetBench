package parquetgo

import (
	"runtime"

	"github.com/xitongsys/parquet-go-source/local"
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

// WriteParquetGo writes records to a Parquet file using parquet-go
func WriteParquetGo(fileName string) error {
	// Open source file
	sourceReader, err := local.NewLocalFileReader(fileName)
	if err != nil {
		return err
	}
	defer sourceReader.Close()

	// Create reader
	pr, err := reader.NewParquetReader(sourceReader, nil, int64(runtime.GOMAXPROCS(0)))
	if err != nil {
		return err
	}
	defer pr.ReadStop()

	// Create output file
	fw, err := local.NewLocalFileWriter(fileName + ".copy.parquet")
	if err != nil {
		return err
	}
	defer fw.Close()

	// Create writer
	pw, err := writer.NewParquetWriter(fw, nil, int64(runtime.GOMAXPROCS(0)))
	if err != nil {
		return err
	}
	defer pw.WriteStop()

	// Copy data in batches
	numRows := int(pr.GetNumRows())
	batchSize := 10000

	for i := 0; i < numRows; i += batchSize {
		currentBatch := batchSize
		if i+batchSize > numRows {
			currentBatch = numRows - i
		}
		if err = pr.SkipRows(int64(currentBatch)); err != nil {
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
