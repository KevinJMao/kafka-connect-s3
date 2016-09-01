package com.deviantart.kafka_connect_s3.writers;

public class Chunk {
  public long rawBytes = 0;
  public long byteOffset = 0;
  public long byteLength = 0;
  public long firstOffset = 0;
  public long numRecords = 0;
}
