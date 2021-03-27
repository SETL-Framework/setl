package io.github.setl.storage;

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SnappyCompressor implements Compressor {

    @Override
    public byte[] compress(String input) throws IOException {
        return Snappy.compress(input, StandardCharsets.UTF_8);
    }

    @Override
    public String decompress(byte[] bytes) throws IOException {
        return Snappy.uncompressString(bytes, StandardCharsets.UTF_8);
    }
}
