package com.deviantart.kafka_connect_s3.writers;

import java.io.IOException;

public interface S3FileWriter {

    long getFirstRecordOffset();

    String getDataFileName();

    String getIndexFileName();

    String getDataFilePath();

    String getIndexFilePath();

    void write(String record) throws IOException;

    void delete() throws IOException;

    void close() throws IOException;

    int getTotalUncompressedSize();

    int getNumChunks();

    int getNumRecords();
}
