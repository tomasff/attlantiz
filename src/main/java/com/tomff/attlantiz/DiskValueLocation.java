package com.tomff.attlantiz;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

public record DiskValueLocation(Path file, long bytesFromStart, Instant writtenOn, int valueSize) {
    public DiskValueLocation {
        Objects.requireNonNull(file);
        Objects.requireNonNull(writtenOn);
    }
}
