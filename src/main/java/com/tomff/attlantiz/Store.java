package com.tomff.attlantiz;

import java.util.Optional;

/**
 * An Attlantiz <a href="https://riak.com/assets/bitcask-intro.pdf">Bitcask</a>-based
 * embedded key-value store with no duplicates.
 */
public interface Store {

    /**
     * Associate a key with a value in the store.
     *
     * @param key   Name of the key
     * @param value Value to be associated with the key
     */
    void put(String key, String value);

    /**
     * Retrieve value associate with the key in the store.
     *
     * @param key Name of the key
     * @return If the key is present in the store, returns an {@link Optional}
     *         describing the value, otherwise an empty {@link Optional}.
     */
    Optional<String> get(String key);

    /**
     * Removes key-value pair from the store if present.
     *
     * @param key Name of the key whose key-value pair is to be removed.
     */
    void remove(String key);

    /**
     * Close the store and flush all pending writes.
     *
     */
    void close();

}
