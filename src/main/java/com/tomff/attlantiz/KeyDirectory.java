package com.tomff.attlantiz;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class KeyDirectory {
    private Map<ByteBuffer, DiskValueLocation> keyLocation;

    public KeyDirectory() {
        keyLocation = new HashMap<>();
    }

    public DiskValueLocation get(ByteBuffer key) {
        return keyLocation.get(key);
    }

    public void put(ByteBuffer key, DiskValueLocation location) {
        keyLocation.put(key, location);
    }

    public boolean containsKey(ByteBuffer key) {
        return keyLocation.containsKey(key);
    }
}
