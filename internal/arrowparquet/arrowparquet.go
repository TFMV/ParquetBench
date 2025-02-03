package arrowparquet

import (
	"context"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	defaultBatchSize    = 64 * 1024 * 1024  // 64MB batch size
	defaultRowGroupSize = 128 * 1024 * 1024 // 128MB row group size
)

// WriteArrowParquet writes records to a Parquet file using Arrow
func WriteArrowParquet(fileName string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
		parquet.WithBatchSize(defaultBatchSize),
		parquet.WithDataPageSize(1*1024*1024), // 1MB page size
		parquet.WithMaxRowGroupLength(defaultRowGroupSize),
	)

	w := file.NewParquetWriter(f, nil, file.WithWriterProps(writerProps))

	defer w.Close()

	return nil
}

// ReadArrowParquet reads records from a Parquet file using Arrow
func ReadArrowParquet(fileName string) error {
	pool := memory.NewGoAllocator()

	// Open the file
	rdr, err := file.OpenParquetFile(fileName, false)
	if err != nil {
		return err
	}
	defer rdr.Close()

	// Create Arrow file reader
	fileReader, err := pqarrow.NewFileReader(rdr, pqarrow.ArrowReadProperties{}, pool)
	if err != nil {
		return err
	}

	// Get record reader for all columns and row groups
	recordReader, err := fileReader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return err
	}
	defer recordReader.Release()

	// Read all records
	for recordReader.Next() {
		record := recordReader.Record()
		record.Release() // Release immediately since we're just reading
	}

	if err := recordReader.Err(); err != nil && err != io.EOF {
		return err
	}

	return nil
}
