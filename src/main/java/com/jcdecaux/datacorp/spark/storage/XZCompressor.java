package com.jcdecaux.datacorp.spark.storage;

import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class XZCompressor implements Compressor {

    @Override
    public byte[] compress(String input) throws IOException {
        if ((input == null) || (input.length() == 0)) {
            return null;
        }
        ByteArrayOutputStream xzOutput = new ByteArrayOutputStream();
        XZOutputStream xzStream = new XZOutputStream(xzOutput, new LZMA2Options(LZMA2Options.PRESET_DEFAULT));
        xzStream.write(input.getBytes(StandardCharsets.UTF_8));
        xzStream.close();
        return xzOutput.toByteArray();
    }

    @Override
    public String decompress(byte[] bytes) throws IOException {
        if ((bytes == null) || (bytes.length == 0)) {
            return "";
        }
        XZInputStream xzInputStream = new XZInputStream(new ByteArrayInputStream(bytes));
        byte firstByte = (byte) xzInputStream.read();
        byte[] buffer = new byte[xzInputStream.available() + 1];
        buffer[0] = firstByte;
        xzInputStream.read(buffer, 1, buffer.length - 1);
        xzInputStream.close();
        return new String(buffer);
    }

}
