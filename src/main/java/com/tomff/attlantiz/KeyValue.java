package com.tomff.attlantiz;

import com.tomff.attlantiz.exceptions.InvalidKeyValueRecordException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.Optional;

public record KeyValue(ByteBuffer key, ByteBuffer value) {
    public KeyValue {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
    }

    public static Optional<KeyValue> load(FileChannel fileChannel, KeyValueHeader header) throws IOException {
        ByteBuffer keyBytes = FileChannelByteBufferReader.readBytes(fileChannel, header.keySize());

        System.out.println(header.keySize() + ", " + keyBytes.position() + ", " + keyBytes.hasRemaining());
        System.out.println(header);

        if (keyBytes.hasRemaining()) {
            throw new InvalidKeyValueRecordException("Invalid key found in KV record.");
        }

        keyBytes.flip();

        ByteBuffer valueBytes = FileChannelByteBufferReader.readBytes(fileChannel, header.valueSize());

        if (valueBytes.hasRemaining()) {
            throw new InvalidKeyValueRecordException("Invalid value found in KV record.");
        }

        valueBytes.flip();

        return Optional.of(
                new KeyValue(
                        keyBytes,
                        valueBytes
                )
        );
    }
}
