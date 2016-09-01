package com.deviantart.kafka_connect_s3.writers;

import com.deviantart.kafka_connect_s3.S3SinkConnectorConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

/**
 * BlockGZIPFileWriter accumulates newline delimited UTF-8 records and writes them to an
 * output file that is readable by GZIP.
 *
 * In fact this file is the concatenation of possibly many separate GZIP files corresponding to smaller chunks
 * of the input. Alongside the output filename.gz file, a file filename-index.json is written containing JSON
 * metadata about the size and location of each block.
 *
 * This allows a reading class to skip to particular line/record without decompressing whole file by looking up
 * the offset of the containing block, seeking to it and beginning GZIp read from there.
 *
 * This is especially useful when the file is an archive in HTTP storage like Amazon S3 where GET request with
 * range headers can allow pulling a small segment from overall compressed file.
 *
 * Note that thanks to GZIP spec, the overall file is perfectly valid and will compress as if it was a single stream
 * with any regular GZIP decoding library or program.
 */
public class BlockGZIPFileWriter implements S3FileWriter {

  private static final Logger log = LoggerFactory.getLogger(BlockGZIPFileWriter.class);
  private String filenameBase;
  private String path;
  private GZIPOutputStream gzipStream;
  private BufferedWriter writer;
  private CountingOutputStream fileStream;
  private ArrayList<Chunk> chunks;

  // Default each chunk is 64MB of uncompressed data
  private long chunkThreshold;

  // Offset to the first record.
  // Set to non-zero if this file is part of a larger stream and you want
  // record offsets in the index to reflect the global offset rather than local
  private long firstRecordOffset;

  static {
    S3FileWriterFactory.getInstance().registerWriter(S3SinkConnectorConstants.S3_OUTPUT_WRITER_BLOCK_GZIP,
            BlockGZIPFileWriter.class);
  }

  public BlockGZIPFileWriter(String filenameBase, String path) throws FileNotFoundException, IOException {
    this(filenameBase, path, 0, 67108864);
  }

  public BlockGZIPFileWriter(String filenameBase, String path, long firstRecordOffset) throws FileNotFoundException, IOException {
    this(filenameBase, path, firstRecordOffset, 67108864);
  }

  public BlockGZIPFileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold)
  throws FileNotFoundException, IOException
  {
    log.info("Initializing BlockGZIPFileWriter");
    this.filenameBase = filenameBase;
    this.path = path;
    this.firstRecordOffset = firstRecordOffset;
    this.chunkThreshold = chunkThreshold;

    chunks = new ArrayList<Chunk>();

    // Initialize first chunk
    Chunk ch = new Chunk();
    ch.firstOffset = firstRecordOffset;
    chunks.add(ch);

    // Explicitly truncate the file. On linux and OS X this appears to happen
    // anyway when opening with FileOutputStream but that behavior is not actually documented
    // or specified anywhere so let's be rigorous about it.
    FileOutputStream fos = new FileOutputStream(new File(getDataFilePath()));
    fos.getChannel().truncate(0);

    // Open file for writing and setup
    this.fileStream = new CountingOutputStream(fos);
    initChunkWriter();
  }

  private void initChunkWriter() throws IOException, UnsupportedEncodingException {
    gzipStream = new GZIPOutputStream(fileStream);
    writer = new BufferedWriter(new OutputStreamWriter(gzipStream, "UTF-8"));
  }

  private Chunk currentChunk() {
    return chunks.get(chunks.size() - 1);
  }

  @Override
  public long getFirstRecordOffset() {
    return firstRecordOffset;
  }

  @Override
  public String getDataFileName() {
    return String.format("%s-%012d.gz", filenameBase, firstRecordOffset);
  }

  @Override
  public String getIndexFileName() {
    return String.format("%s-%012d.index.json", filenameBase, firstRecordOffset);
  }

  @Override
  public String getDataFilePath() {
    return String.format("%s/%s", path, this.getDataFileName());
  }

  @Override
  public String getIndexFilePath() {
    return String.format("%s/%s", path, this.getIndexFileName());
  }

  /**
   * Writes string to file, assuming this is a single record
   *
   * If there is no newline at then end we will add one
   */
  @Override
  public void write(String record) throws IOException {
    Chunk ch = currentChunk();

    boolean hasNewLine = record.endsWith("\n");

    int rawBytesToWrite = record.length();
    if (!hasNewLine) {
      rawBytesToWrite += 1;
    }

    if ((ch.rawBytes + rawBytesToWrite) > chunkThreshold) {
      finishChunk();
      initChunkWriter();

      Chunk newCh = new Chunk();
      newCh.firstOffset = ch.firstOffset + ch.numRecords;
      newCh.byteOffset = ch.byteOffset + ch.byteLength;
      chunks.add(newCh);
      ch = newCh;
    }

    writer.append(record);
    if (!hasNewLine) {
      writer.newLine();
    }
    ch.rawBytes += rawBytesToWrite;
    ch.numRecords++;
  }

  @Override
  public void delete() throws IOException {
    deleteIfExists(getDataFilePath());
    deleteIfExists(getIndexFilePath());
  }

  private void deleteIfExists(String path) throws IOException {
    File f = new File(path);
    if (f.exists() && !f.isDirectory()) {
      f.delete();
    }
  }

  private void finishChunk() throws IOException {
    Chunk ch = currentChunk();

    // Complete GZIP block without closing stream
    writer.flush();
    gzipStream.finish();

    // We can no find out how long this chunk was compressed
    long bytesWritten = fileStream.getNumBytesWritten();
    ch.byteLength = bytesWritten - ch.byteOffset;
  }

  @Override
  public void close() throws IOException {
    // Flush last chunk, updating index
    finishChunk();
    // Now close the writer (and the whole stream stack)
    writer.close();
    writeIndex();
  }

  private void writeIndex() throws IOException {
    JSONArray chunkArr = new JSONArray();

    for (Chunk ch : chunks) {
      JSONObject chunkObj = new JSONObject();
      chunkObj.put("first_record_offset", ch.firstOffset);
      chunkObj.put("num_records", ch.numRecords);
      chunkObj.put("byte_offset", ch.byteOffset);
      chunkObj.put("byte_length", ch.byteLength);
      chunkObj.put("byte_length_uncompressed", ch.rawBytes);
      chunkArr.add(chunkObj);
    }

    JSONObject index = new JSONObject();
    index.put("chunks", chunkArr);

    try (FileWriter file = new FileWriter(getIndexFilePath())) {
      file.write(index.toJSONString());
      file.close();
    }
  }

  @Override
  public int getTotalUncompressedSize() {
    int totalBytes = 0;
    for (Chunk ch : chunks) {
      totalBytes += ch.rawBytes;
    }
    return totalBytes;
  }

  @Override
  public int getNumChunks() {
    return chunks.size();
  }

  @Override
  public int getNumRecords() {
    int totalRecords = 0;
    for (Chunk ch : chunks) {
      totalRecords += ch.numRecords;
    }
    return totalRecords;
  }
}