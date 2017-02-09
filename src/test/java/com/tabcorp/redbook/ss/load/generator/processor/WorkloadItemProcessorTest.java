package com.tabcorp.redbook.ss.load.generator.processor;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.*;

/**
 * Created by agrawald on 7/02/17.
 */
public class WorkloadItemProcessorTest {
    private String payload = "Hello World hi";
    private WorkloadItemProcessor processor;

    @org.junit.Test
    public void compressAndEncode() throws Exception {
        //�H����/�I
        //�H����/�IQ��
        Deflater deflater = new Deflater();
        deflater.setInput(payload.getBytes(StandardCharsets.UTF_8));
        deflater.finish();

        byte[] bytesCompressed = new byte[Short.MAX_VALUE];
        int numberOfBytesAfterCompression = deflater.deflate(bytesCompressed);
        byte[] returnValues = new byte[numberOfBytesAfterCompression];
        System.arraycopy
                (
                        bytesCompressed,
                        2,
                        returnValues,
                        0,
                        numberOfBytesAfterCompression-6
                );

        System.out.println(new String(returnValues));
    }

}