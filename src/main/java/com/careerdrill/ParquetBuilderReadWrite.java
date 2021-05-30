package com.careerdrill;

import com.careerdrill.entity.Employee;
import com.careerdrill.util.FooterUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.PositionOutputStream;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.careerdrill.io.InputFile.nioPathToInputFile;
import static com.careerdrill.io.OutputFile.nioPathToOutputFile;

/*
    Example of reading writing Parquet in java without BigData tools.
*/

public class ParquetBuilderReadWrite {
    public static void main(String[] args) {
        try {
            final Schema schema = Employee.getClassSchema();
            final Path parquetFilePath = FileSystems.getDefault().getPath("paraquet-gen/employee2.parquet");

            Files.deleteIfExists(parquetFilePath);

            writeToParquet(schema, parquetFilePath);
            readFromParquet(parquetFilePath);

            FooterUtil.extractMetaDataFooter(parquetFilePath);

            Path parentDirPath = parquetFilePath.getParent();
            if (parentDirPath == null) {
                parentDirPath = FileSystems.getDefault().getPath(".");
            }

            final Path emptyParquetFilePath = Paths.get(parentDirPath.toString(), "empty.parquet");
            //noinspection EmptyTryBlock
            try (final ParquetWriter<GenericData.Record> ignored = createParquetWriterInstance(schema, emptyParquetFilePath)) {
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }


    private static ParquetWriter createParquetWriterInstance(@Nonnull final Schema schema,
                                                             @Nonnull final Path fileToWrite)
            throws IOException {
        return AvroParquetWriter
                .builder(nioPathToOutputFile(fileToWrite))
                .withRowGroupSize(256 * 1024 * 1024)
                .withPageSize(128 * 1024)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.GZIP)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build();
    }

    private static void writeToParquet(@Nonnull final Schema schema,
                                       @Nonnull final Path fileToWrite
    ) throws IOException {

        Employee e1 = EmployeeBuilder.buildEmployee();

        try (final ParquetWriter writer = createParquetWriterInstance(schema, fileToWrite)) {
            writer.write(e1);
            writer.close();
            final Path metaDataOutPath = Paths.get(ParquetFileWriter.PARQUET_METADATA_FILE);
            Files.deleteIfExists(metaDataOutPath);
            try (final PositionOutputStream out = nioPathToOutputFile(metaDataOutPath).createOrOverwrite(0)) {
                FooterUtil.serializeFooter(writer.getFooter(), out);
            }
        }
    }


    private static void readFromParquet(@Nonnull final Path filePathToRead) throws IOException {
        try (final ParquetReader reader = AvroParquetReader
                .builder(nioPathToInputFile(filePathToRead))
                .withConf(new Configuration())
                .build()) {

            Employee record;
            while ((record = (Employee) reader.read()) != null) {
                System.out.println("Read Employee:" + record);
            }
        }
    }


}