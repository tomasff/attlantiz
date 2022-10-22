package com.tomff.attlantiz;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * An Attlantiz <a href="https://riak.com/assets/bitcask-intro.pdf">Bitcask</a>-based
 * embedded key-value store with no duplicates.
 */
public interface Store {

    /**
     * Associate a key with a value in the store.
     *
     * @param key   Key
     * @param value Value to be associated with the key
     */
    void put(ByteBuffer key, ByteBuffer value);

    /**
     * Retrieve value associate with the key in the store.
     *
     * @param key Key
     * @return If the key is present in the store, returns an {@link Optional}
     * describing the value, otherwise an empty {@link Optional}.
     */
    Optional<ByteBuffer> get(ByteBuffer key);

    /**
     * Removes key-value pair from the store if present.
     *
     * @param key The key whose key-value pair is to be removed.
     */
    void remove(ByteBuffer key);

    /**
     * Close the store and flush all pending writes.
     *
     */
    void close();

}
