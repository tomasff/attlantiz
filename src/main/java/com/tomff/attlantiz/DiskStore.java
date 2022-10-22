package com.tomff.attlantiz;

import com.tomff.attlantiz.exceptions.InvalidKeyValueRecordException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    private KeyDirectory keyDirectory;

    private int fileBytesSizeThreshold;

    public DiskStore(Path directory) throws IOException {
        this(directory, DEFAULT_FILE_BYTES_SIZE_THRESHOLD);
    }

    public DiskStore(Path directory, int fileBytesSizeThreshold) throws IOException {
        storeDirectory = Objects.requireNonNull(
                directory,
                "The directory path for an Attlantiz DiskStore must not be null."
        );

        keyDirectory = new KeyDirectory();

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
        Files.walkFileTree(storeDirectory, new KeyDirectoryFileVisitor(keyDirectory));
    }

    private void put(ByteBuffer key, ByteBuffer value, boolean isTombstone) {
        DiskValueLocation valueLocation = keyDirectory.get(key);

        Instant now = Instant.now();

        ByteBuffer keyValueBytes = ByteBuffer.allocate(
                KeyValueHeader.HEADER_SIZE + key.capacity() + value.capacity()
        );

        keyValueBytes.putLong(now.toEpochMilli());
        keyValueBytes.put((byte) (isTombstone ? 1 : 0));
        keyValueBytes.putInt(key.capacity());
        keyValueBytes.putInt(value.capacity());
        keyValueBytes.put(key);
        keyValueBytes.put(value);

        keyValueBytes.flip();

        // Restore the position of the key and value buffers
        key.flip();
        value.flip();

        try {
            valueLocation = new DiskValueLocation(activeFile,
                    activeFileChannel.position(),
                    now,
                    value.capacity()
            );

            while (keyValueBytes.hasRemaining()) {
                activeFileChannel.write(keyValueBytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        keyDirectory.put(key, valueLocation);
    }

    @Override
    public void put(ByteBuffer key, ByteBuffer value) {
        put(key, value, false);
    }

    @Override
    public Optional<ByteBuffer> get(ByteBuffer key) {
        DiskValueLocation location = keyDirectory.get(key);

        if (location == null) {
            return Optional.empty();
        }

        Optional<KeyValue> diskKeyValue = Optional.empty();

        try (FileChannel fc = FileChannel.open(location.file(), READ)) {
            Optional<KeyValueHeader> header = KeyValueHeader.load(fc, location);

            if (header.isEmpty()) {
                throw new InvalidKeyValueRecordException("Invalid KV header found");
            }

            if (header.get().isTombstone()) {
                return Optional.empty();
            }

            diskKeyValue = KeyValue.load(fc, header.get());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return diskKeyValue.map(KeyValue::value);
    }

    @Override
    public void remove(ByteBuffer key) {
        if (!keyDirectory.containsKey(key)) {
            return;
        }

        put(key, ByteBuffer.allocate(0).flip(), true);
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