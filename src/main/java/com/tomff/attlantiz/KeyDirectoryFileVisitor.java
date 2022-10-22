package com.tomff.attlantiz;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.StandardOpenOption.READ;

public class KeyDirectoryFileVisitor extends SimpleFileVisitor<Path> {

    private final KeyDirectory keyDirectory;

    public KeyDirectoryFileVisitor(KeyDirectory keyDirectory) {
        Objects.requireNonNull(keyDirectory);

        this.keyDirectory = keyDirectory;

        System.out.println("Loading!");
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Objects.requireNonNull(file);
        Objects.requireNonNull(attrs);

        if (!attrs.isRegularFile()) {
            return CONTINUE;
        }

        try (FileChannel fc = FileChannel.open(file, READ)) {
            Optional<KeyValueHeader> possibleKeyValueHeader = KeyValueHeader.load(fc);

            while (possibleKeyValueHeader.isPresent()) {
                KeyValueHeader keyValueHeader = possibleKeyValueHeader.get();
                Optional<KeyValue> possibleKeyValue = KeyValue.load(fc, keyValueHeader);

                if (possibleKeyValue.isEmpty()) {
                    break;
                }

                KeyValue keyValue = possibleKeyValue.get();

                System.out.println("loading key: " + keyValue.key());

                if (keyDirectory.containsKey(keyValue.key())) {
                    DiskValueLocation existingKeyValueHeader = keyDirectory.get(keyValue.key());

                    if (existingKeyValueHeader.writtenOn().isAfter(keyValueHeader.writtenOn())) {
                        continue;
                    }
                }

                keyDirectory.put(keyValue.key(),
                        new DiskValueLocation(
                                file,
                                fc.position() - keyValueHeader.totalSize(),
                                keyValueHeader.writtenOn(),
                                keyValueHeader.valueSize()
                        )
                );

                possibleKeyValueHeader = KeyValueHeader.load(fc);
            }
        }

        return CONTINUE;
    }
}
