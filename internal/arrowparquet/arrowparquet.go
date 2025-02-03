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

// WriteArrowParquet writes records to a Parquet file using Arrow
func WriteArrowParquet(fileName string) error {
	pool := memory.NewGoAllocator()

	// First open source file
	sourceReader, err := file.OpenParquetFile(fileName, false)
	if err != nil {
		return err
	}
	defer sourceReader.Close()

	// Create Arrow file reader for the source
	sourceFileReader, err := pqarrow.NewFileReader(sourceReader, pqarrow.ArrowReadProperties{}, pool)
	if err != nil {
		return err
	}

	// Get schema from source
	schema, err := sourceFileReader.Schema()
	if err != nil {
		return err
	}

	// Create output file
	f, err := os.Create(fileName + ".copy.parquet")
	if err != nil {
		return err
	}
	defer f.Close()

	// Create Arrow writer
	w, err := pqarrow.NewFileWriter(schema, f, parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
		parquet.WithDictionaryDefault(true),
		parquet.WithDataPageSize(1*1024*1024),
	), pqarrow.ArrowWriterProperties{})
	if err != nil {
		return err
	}
	defer w.Close()

	// Get record reader from source
	recordReader, err := sourceFileReader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return err
	}
	defer recordReader.Release()

	// Write records
	for recordReader.Next() {
		record := recordReader.Record()
		if err := w.WriteBuffered(record); err != nil {
			return err
		}
		record.Release()
	}

	// Check for errors, ignoring EOF
	if err := recordReader.Err(); err != nil && err != io.EOF {
		return err
	}

	// Ensure all buffered records are written
	return w.Close()
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
