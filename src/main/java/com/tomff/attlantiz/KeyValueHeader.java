package com.tomff.attlantiz;

import com.tomff.attlantiz.exceptions.InvalidKeyValueRecordException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public record KeyValueHeader(Instant writtenOn, int keySize, int valueSize) {

    /**
     * A header for a key-value pair includes a
     * timestamp of when the record was created (8 bytes),
     * the key size (4 bytes), and the value size (4 bytes)
     */
    public static final int HEADER_SIZE = 8 + 4 + 4;

    public KeyValueHeader {
        Objects.requireNonNull(writtenOn);
    }

    public int totalSize() {
        return HEADER_SIZE + keySize + valueSize;
    }

    public static Optional<KeyValueHeader> load(FileChannel fileChannel, DiskValueLocation location) throws IOException {
        fileChannel.position(location.bytesFromStart());

        return load(fileChannel);
    }

    public static Optional<KeyValueHeader> load(FileChannel fileChannel) throws IOException {
        if (fileChannel.position() == fileChannel.size()) {
            return Optional.empty();
        }

        ByteBuffer valueHeaderBytes = FileChannelByteBufferReader.readBytes(fileChannel, HEADER_SIZE);

        if (valueHeaderBytes.hasRemaining()) {
            throw new InvalidKeyValueRecordException("Invalid key-value pair header found.");
        }

        valueHeaderBytes.flip();

        Instant writtenOn = Instant.ofEpochMilli(valueHeaderBytes.getLong());
        int keySize = valueHeaderBytes.getInt();
        int valueSize = valueHeaderBytes.getInt();

        return Optional.of(new KeyValueHeader(writtenOn, keySize, valueSize));
    }
}
