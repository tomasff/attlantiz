package com.tomff.attlantiz;

import java.io.IOException;
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

    private final Map<String, DiskValueLocation> keyDirectory;

    public KeyDirectoryFileVisitor(Map<String, DiskValueLocation> keyDirectory) {
        Objects.requireNonNull(keyDirectory);

        this.keyDirectory = keyDirectory;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Objects.requireNonNull(file);
        Objects.requireNonNull(attrs);

        if (!attrs.isRegularFile()) {
            return CONTINUE;
        }

        try (FileChannel fc = FileChannel.open(file, READ)) {
            Optional<KeyValueHeader> keyValueHeader = KeyValueHeader.load(fc);

            while (keyValueHeader.isPresent()) {
                KeyValueHeader kvHeader = keyValueHeader.get();
                Optional<KeyValue> keyValue = KeyValue.load(fc, kvHeader);

                if (keyValue.isEmpty()) {
                    break;
                }

                keyDirectory.put(keyValue.get().key(),
                        new DiskValueLocation(
                                file,
                                fc.position() - kvHeader.totalSize(),
                                kvHeader.writtenOn(),
                                kvHeader.valueSize()
                        )
                );

                keyValueHeader = KeyValueHeader.load(fc);
            }
        }

        return CONTINUE;
    }
}
