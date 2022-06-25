package com.tomff.attlantiz;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DiskStoreTests {

    @TempDir
    private Path temporaryStoreDir;

    private DiskStore buildDiskStore() {
        return new DiskStore(temporaryStoreDir.resolve("store.attz"));
    }

    @Test
    public void putAndGet() {
        DiskStore store = buildDiskStore();

        store.put("key", "value");

        assertEquals(Optional.of("value"), store.get("key"));
        store.close();
    }


    @Test
    public void putAndGetMultiple() {
        DiskStore store = buildDiskStore();

        for (int i = 0; i < 1000; i++) {
            store.put("key" + i, "value" + i);
        }

        for (int i = 0; i < 1000; i++) {
            assertEquals(Optional.of("value" + i), store.get("key" + i));
        }

        store.close();
    }

    @Test
    public void getNonexistentKeyValue() {
        DiskStore store = buildDiskStore();

        assertEquals(Optional.empty(), store.get("key"));
        store.close();
    }

    @Test
    public void putAndGetAndRemove() {
        DiskStore store = buildDiskStore();

        store.put("key", "value");

        assertEquals(Optional.of("value"), store.get("key"));
        assertEquals(Optional.of("value"), store.remove("key"));
        assertEquals(Optional.empty(), store.get("key"));

        store.close();
    }

    @Test
    public void putAndGetAndRemoveMultiple() {
        DiskStore store = buildDiskStore();

        for (int i = 0; i < 1000; i++) {
            store.put("key" + i, "value" + i);
        }

        for (int i = 0; i < 1000; i++) {
            assertEquals(Optional.of("value" + i), store.get("key" + i));
            assertEquals(Optional.of("value" + i), store.remove("key" + i));
            assertEquals(Optional.empty(), store.get("key" + i));
        }

        store.close();
    }

    @Test
    public void putIsPersistent() {
        DiskStore store = buildDiskStore();

        store.put("key", "value");
        assertEquals(Optional.of("value"), store.get("key"));

        store.close();

        store = buildDiskStore();
        assertEquals(Optional.of("value"), store.get("key"));

        store.close();
    }

    @Test
    public void putAndRemoveIsPersistent() {
        DiskStore store = buildDiskStore();

        store.put("key", "value");
        assertEquals(Optional.of("value"), store.get("key"));

        store.close();

        store = buildDiskStore();
        assertEquals(Optional.of("value"), store.get("key"));
        assertEquals(Optional.of("value"), store.remove("key"));
        assertEquals(Optional.empty(), store.get("key"));

        store.close();

        store = buildDiskStore();
        assertEquals(Optional.empty(), store.get("key"));
        assertEquals(Optional.empty(), store.remove("key"));

        store.close();
    }
}
