package com.tomff.attlantiz;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

public class DiskStore implements Store {

    private final Path file;

    public DiskStore(Path file) {
        this.file = Objects.requireNonNull(
                file,
                "The file path for an Attlantiz DiskStore must not be null."
        );
    }

    @Override
    public void put(String key, String value) {

    }

    @Override
    public Optional<String> get(String key) {
        return Optional.empty();
    }

    @Override
    public Optional<String> remove(String key) {
        return Optional.empty();
    }

    @Override
    public void close() {

    }
}
