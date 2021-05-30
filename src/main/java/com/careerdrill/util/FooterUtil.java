package com.careerdrill.util;

import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static com.careerdrill.io.InputFile.nioPathToInputFile;
import static com.careerdrill.io.OutputFile.nioPathToOutputFile;

public class FooterUtil {

    public static void extractMetaDataFooter(final Path parquetFilePath) throws IOException {
        try (final ParquetFileReader rdr = ParquetFileReader.open(nioPathToInputFile(parquetFilePath))) {
            final ParquetMetadata footer = rdr.getFooter();
            final Path metaDataOutPath = Paths.get(ParquetFileWriter.PARQUET_METADATA_FILE + "_dup.parquet");
            Files.deleteIfExists(metaDataOutPath);
            try (final PositionOutputStream out = nioPathToOutputFile(metaDataOutPath).createOrOverwrite(0)) {
                serializeFooter(footer, out);
            }
        }
    }

    public static void serializeFooter(ParquetMetadata footer, final PositionOutputStream out) throws IOException {
        out.write(ParquetFileWriter.MAGIC);
        final long footerIndex = out.getPos();
        //noinspection unchecked
        footer = new ParquetMetadata(footer.getFileMetaData(), Collections.EMPTY_LIST);
        final ParquetMetadataConverter metadataConverter = new ParquetMetadataConverter();
        final org.apache.parquet.format.FileMetaData parquetMetadata =
                metadataConverter.toParquetMetadata(ParquetFileWriter.CURRENT_VERSION, footer);
        Util.writeFileMetaData(parquetMetadata, out);
        BytesUtils.writeIntLittleEndian(out, (int) (out.getPos() - footerIndex));
        out.write(ParquetFileWriter.MAGIC);
    }
}
