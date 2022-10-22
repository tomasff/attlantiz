package com.tomff.attlantiz;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DiskStoreTests {

    @TempDir
    private Path temporaryStoreDir;

    private ByteBuffer getIntByteBuffer(int num) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(num).flip();
    }

    @Test
    public void putAndGet() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        ByteBuffer key = getIntByteBuffer(1);
        ByteBuffer value = getIntByteBuffer(2);

        store.put(key, value);

        assertEquals(Optional.of(value), store.get(key));

        store.close();
    }

    @Test
    public void putAndGetMultiple() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        for (int i = 0; i < 1000; i++) {
            store.put(getIntByteBuffer(i), getIntByteBuffer(i + 1));
        }

        for (int i = 0; i < 1000; i++) {
            assertEquals(Optional.of(getIntByteBuffer(i + 1)), store.get(getIntByteBuffer(i)));
        }

        store.close();
    }

    @Test
    public void getNonexistentKeyValue() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        assertEquals(Optional.empty(), store.get(getIntByteBuffer(1)));
        store.close();
    }

    @Test
    public void putAndGetAndRemove() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        ByteBuffer key = getIntByteBuffer(1);
        ByteBuffer value = getIntByteBuffer(2);

        store.put(key, value);

        assertEquals(Optional.of(value), store.get(key));

        store.remove(key);
        assertEquals(Optional.empty(), store.get(key));

        store.close();
    }

    @Test
    public void putAndGetAndRemoveMultiple() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        for (int i = 0; i < 1000; i++) {
            store.put(getIntByteBuffer(i), getIntByteBuffer(i + 1));
        }

        for (int i = 0; i < 1000; i++) {
            ByteBuffer key = getIntByteBuffer(i);
            ByteBuffer value = getIntByteBuffer(i + 1);

            assertEquals(Optional.of(value), store.get(key));
            store.remove(key);
            assertEquals(Optional.empty(), store.get(key));
        }

        store.close();
    }

    @Test
    public void putIsPersistent() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        ByteBuffer key = getIntByteBuffer(1);
        ByteBuffer value = getIntByteBuffer(2);

        store.put(key, value);
        assertEquals(Optional.of(value), store.get(key));

        store.close();

        store = new DiskStore(temporaryStoreDir);
        assertEquals(Optional.of(value), store.get(key));

        store.close();
    }

    @Test
    public void putAndRemoveIsPersistent() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        ByteBuffer key = getIntByteBuffer(1);
        ByteBuffer value = getIntByteBuffer(2);

        store.put(key, value);
        assertEquals(Optional.of(value), store.get(key));

        store.close();

        store = new DiskStore(temporaryStoreDir);
        assertEquals(Optional.of(value), store.get(key));
        store.remove(key);
        assertEquals(Optional.empty(), store.get(key));

        store.close();

        store = new DiskStore(temporaryStoreDir);
        assertEquals(Optional.empty(), store.get(key));

        store.close();
    }

    @Test
    public void removeDoesntAddRecord() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        ByteBuffer key = getIntByteBuffer(1);

        assertEquals(Optional.empty(), store.get(key));
        store.remove(key);
        assertEquals(Optional.empty(), store.get(key));

        store.close();
    }

    @Test
    public void putMultipleSameKey() throws IOException {
        DiskStore store = new DiskStore(temporaryStoreDir);

        ByteBuffer key = getIntByteBuffer(2022);

        store.put(key, getIntByteBuffer(1));
        store.put(key, getIntByteBuffer(2));
        store.put(key, getIntByteBuffer(3));
        store.put(key, getIntByteBuffer(4));

        assertEquals(Optional.of(getIntByteBuffer(4)), store.get(key));

        store.close();

        store = new DiskStore(temporaryStoreDir);
        assertEquals(Optional.of(getIntByteBuffer(4)), store.get(key));
    }
}
