package com.deviantart.kafka_connect_s3.writers;

import com.deviantart.kafka_connect_s3.S3SinkConnectorConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;

public class PlaintextFileWriter implements S3FileWriter {

    private static final Logger log = LoggerFactory.getLogger(PlaintextFileWriter.class);
    private String filenameBase;
    private String path;
    private BufferedWriter writer;
    private CountingOutputStream countingOutputStream;
    private ArrayList<Chunk> chunks;
    private long chunkThreshold;
    private long firstRecordOffset;

    static {
        S3FileWriterFactory.getInstance().registerWriter(S3SinkConnectorConstants.S3_OUTPUT_WRITER_PLAINTEXT,
                PlaintextFileWriter.class);
    }

    public PlaintextFileWriter(String filenameBase, String path) throws FileNotFoundException, IOException {
        this(filenameBase, path, 0, 67108864);
    }

    public PlaintextFileWriter(String filenameBase, String path, long firstRecordOffset)
            throws FileNotFoundException, IOException {
        this(filenameBase, path, firstRecordOffset, 67108864);
    }


    public PlaintextFileWriter(String filenameBase, String path, long firstRecordOffset, long chunkThreshold)
            throws FileNotFoundException, IOException {
        log.info("Initializing PlaintextFileWriter.");
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
        this.countingOutputStream = new CountingOutputStream(fos);
        initChunkWriter();
    }

    private void initChunkWriter() throws IOException {
        writer = new BufferedWriter(new OutputStreamWriter(countingOutputStream, "UTF-8"));
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
        return String.format("%s-%012d.log", filenameBase, firstRecordOffset);
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

    @Override
    public void write(String record) throws IOException {
        //TODO Implement
        Chunk ch = currentChunk();

        String formattedRecord = record.trim();
        int rawBytestoWrite = formattedRecord.length() + 1;

        if ((ch.rawBytes + rawBytestoWrite) > chunkThreshold) {
            finishChunk();
            initChunkWriter();

            Chunk newCh = new Chunk();
            newCh.firstOffset = ch.firstOffset + ch.numRecords;
            newCh.byteOffset = ch.byteOffset + ch.byteLength;
            chunks.add(newCh);
            ch = newCh;
        }

        writer.append(record);
        writer.newLine();
        ch.rawBytes += rawBytestoWrite;
        ch.numRecords++;
    }

    private void finishChunk() throws IOException {
        Chunk ch = currentChunk();
        writer.flush();

        long bytesWritten = countingOutputStream.getNumBytesWritten();
        ch.byteLength = bytesWritten - ch.byteOffset;
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

    @Override
    public void close() throws IOException {
        finishChunk();

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
