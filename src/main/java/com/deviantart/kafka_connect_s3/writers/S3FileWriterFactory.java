package com.deviantart.kafka_connect_s3.writers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.HashMap;

public class S3FileWriterFactory {
    private static final Logger log = LoggerFactory.getLogger(S3FileWriterFactory.class);

    private static S3FileWriterFactory instance;

    private HashMap<String, Class> registeredWriters = new HashMap<>();

    private S3FileWriterFactory() { }

    public static synchronized S3FileWriterFactory getInstance() {
        if (instance == null) {
            instance = new S3FileWriterFactory();
        }
        return instance;
    }

    public void registerWriter(String writerClassName, Class writerClass) {
        registeredWriters.put(writerClassName, writerClass);
    }

    public S3FileWriter createWriter(String writerClassName, String filenameBase, String path, long firstRecordOffset,
                                     long chunkThreshold) throws InstantiationException {
        try {
            Class writerClass = registeredWriters.get(writerClassName);
            Constructor writerConstructor = writerClass.getDeclaredConstructor(
                    new Class[]{String.class, String.class, long.class, long.class});
            return (S3FileWriter) writerConstructor.newInstance(filenameBase, path, firstRecordOffset, chunkThreshold);
        } catch (ReflectiveOperationException ex) {
            log.error("Error instantiating S3FileWriter class {}:", writerClassName, ex);
            throw new InstantiationException();
        }
    }


}
