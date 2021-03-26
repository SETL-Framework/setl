package io.github.setl.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * XZCompressor implement {@link Compressor}'s interface with the GZIP compression algorithm
 */
public class GZIPCompressor implements Compressor {

    @Override
    public byte[] compress(String input) throws IOException {
        if ((input == null) || (input.length() == 0)) {
            return null;
        }
        ByteArrayOutputStream compressedBytes = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutput = new GZIPOutputStream(compressedBytes);
        gzipOutput.write(input.getBytes(StandardCharsets.UTF_8));
        gzipOutput.flush();
        gzipOutput.close();
        return compressedBytes.toByteArray();
    }

    @Override
    public String decompress(byte[] bytes) throws IOException {
        if ((bytes == null) || (bytes.length == 0)) {
            return "";
        }

        InputStreamReader inputStreamReader;
        if (isCompressed(bytes)) {
            GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(bytes));
            inputStreamReader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
        } else {
            inputStreamReader = new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8);
        }

        return new BufferedReader(inputStreamReader).lines().collect(Collectors.joining("\n"));
    }

    private static boolean isCompressed(final byte[] compressed) {
        return (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) &&
                (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }
}

