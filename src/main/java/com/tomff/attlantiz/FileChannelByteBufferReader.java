package com.tomff.attlantiz;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;

public class FileChannelByteBufferReader {

    public static ByteBuffer readBytes(FileChannel fileChannel, int bytes) throws IOException {
        Objects.requireNonNull(fileChannel);

        ByteBuffer buffer = ByteBuffer.allocate(bytes);

        int bytesRead;

        do {
            bytesRead = fileChannel.read(buffer);
        } while (bytesRead != -1 && buffer.hasRemaining());

        return buffer;
    }
}
