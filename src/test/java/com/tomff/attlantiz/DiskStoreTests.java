package com.tomff.attlantiz;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DiskStoreTests {

    @TempDir
    private Path temporaryStoreDir;

    @Test
    public void putAndGet() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        store.put("key", "value");

        assertEquals(Optional.of("value"), store.get("key"));
        store.close();
    }

    @Test
    public void putAndGetMultiple() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        for (int i = 0; i < 1000; i++) {
            store.put("key" + i, "value" + i);
        }

        for (int i = 0; i < 1000; i++) {
            assertEquals(Optional.of("value" + i), store.get("key" + i));
        }

        store.close();
    }

    @Test
    public void getNonexistentKeyValue() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        assertEquals(Optional.empty(), store.get("key"));
        store.close();
    }

    @Test
    public void putAndGetAndRemove() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        store.put("key", "value");

        assertEquals(Optional.of("value"), store.get("key"));

        store.remove("key");
        assertEquals(Optional.empty(), store.get("key"));

        store.close();
    }

    @Test
    public void putAndGetAndRemoveMultiple() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        for (int i = 0; i < 1000; i++) {
            store.put("key" + i, "value" + i);
        }

        for (int i = 0; i < 1000; i++) {
            assertEquals(Optional.of("value" + i), store.get("key" + i));
            store.remove("key" + i);
            assertEquals(Optional.empty(), store.get("key" + i));
        }

        store.close();
    }

    @Test
    public void putIsPersistent() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        store.put("key", "value");
        assertEquals(Optional.of("value"), store.get("key"));

        store.close();

        store = new DiskStore(temporaryStoreDir);
        assertEquals(Optional.of("value"), store.get("key"));

        store.close();
    }

    @Test
    public void putAndRemoveIsPersistent() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        store.put("key", "value");
        assertEquals(Optional.of("value"), store.get("key"));

        store.close();

        store = new DiskStore(temporaryStoreDir);
        assertEquals(Optional.of("value"), store.get("key"));
        store.remove("key");
        assertEquals(Optional.empty(), store.get("key"));

        store.close();

        store = new DiskStore(temporaryStoreDir);
        assertEquals(Optional.empty(), store.get("key"));

        store.close();
    }

    @Test
    public void removeDoesntAddRecord() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        assertEquals(Optional.empty(), store.get("key"));
        store.remove("key");
        assertEquals(Optional.empty(), store.get("key"));

        store.close();
    }
}
