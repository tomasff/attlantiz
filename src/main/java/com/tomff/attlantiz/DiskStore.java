package com.tomff.attlantiz;

import com.tomff.attlantiz.exceptions.InvalidKeyValueRecordException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;

import static java.nio.file.StandardOpenOption.*;

public class DiskStore implements Store {

    private static final int DEFAULT_FILE_BYTES_SIZE_THRESHOLD = 1_000_000;

    private final Path storeDirectory;

    private Path activeFile;
    private FileChannel activeFileChannel;

    private Map<String, DiskValueLocation> keyDiskLocations;

    private int fileBytesSizeThreshold;

    public DiskStore(Path directory) throws IOException {
        this(directory, DEFAULT_FILE_BYTES_SIZE_THRESHOLD);
    }

    public DiskStore(Path directory, int fileBytesSizeThreshold) throws IOException {
        storeDirectory = Objects.requireNonNull(
                directory,
                "The directory path for an Attlantiz DiskStore must not be null."
        );

        keyDiskLocations = new HashMap<>();

        this.fileBytesSizeThreshold = fileBytesSizeThreshold;

        activeFile = storeDirectory.resolve(UUID.randomUUID().toString());

        if (Files.notExists(directory)) {
            Files.createDirectory(directory);
        } else {
            loadExistingFiles();
        }

        Files.createFile(activeFile);
        activeFileChannel = FileChannel.open(activeFile, READ, WRITE);
    }

    private void loadExistingFiles() throws IOException {
        Files.walkFileTree(storeDirectory, new KeyDirectoryFileVisitor(keyDiskLocations));
    }

    @Override
    public void put(String key, String value) {
        DiskValueLocation valueLocation = keyDiskLocations.get(key);

        Instant now = Instant.now();
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        ByteBuffer keyValueBytes = ByteBuffer.allocate(
                KeyValueHeader.HEADER_SIZE + keyBytes.length + valueBytes.length
        );

        keyValueBytes.putLong(now.toEpochMilli());
        keyValueBytes.putInt(keyBytes.length);
        keyValueBytes.putInt(valueBytes.length);
        keyValueBytes.put(keyBytes);
        keyValueBytes.put(valueBytes);

        keyValueBytes.flip();

        try {
            valueLocation = new DiskValueLocation(activeFile,
                    activeFileChannel.position(),
                    now,
                    valueBytes.length
            );

            while (keyValueBytes.hasRemaining()) {
                activeFileChannel.write(keyValueBytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        keyDiskLocations.put(key, valueLocation);
    }

    @Override
    public Optional<String> get(String key) {
        DiskValueLocation location = keyDiskLocations.get(key);

        if (location == null) {
            return Optional.empty();
        }

        Optional<KeyValue> diskKeyValue = Optional.empty();

        try (FileChannel fc = FileChannel.open(location.file(), READ)) {
            Optional<KeyValueHeader> header = KeyValueHeader.load(fc, location);

            if (header.isEmpty()) {
                throw new InvalidKeyValueRecordException("Invalid KV header found");
            }

            diskKeyValue = KeyValue.load(fc, header.get());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return diskKeyValue.map(KeyValue::value);
    }

    @Override
    public Optional<String> remove(String key) {
        return Optional.empty();
    }

    @Override
    public void close() {
        try {
            this.activeFileChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}