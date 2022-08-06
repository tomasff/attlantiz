package com.tomff.attlantiz;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

public record DiskValueLocation(Path file, long bytesFromStart, Instant lastModifiedAt, int valueSize) {
    public DiskValueLocation {
        Objects.requireNonNull(file);
        Objects.requireNonNull(lastModifiedAt);
    }
}
