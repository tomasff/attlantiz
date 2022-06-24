package com.tomff.attlantiz;

import java.util.Optional;

public interface Store {

    /**
     * Associate a key with a value in the store.
     *
     * @param key   Name of the key
     * @param value Value to be associated with the key
     */
    public void put(String key, String value);

    /**
     * Retrieve value associate with the key in the store.
     *
     * @param key Name of the key
     * @return If the key is present in the store, returns an {@link Optional}
     *         describing the value, otherwise an empty {@link Optional}.
     */
    public Optional<String> get(String key);

    /**
     * Close the store and flush all pending writes.
     *
     */
    public void close();

}
