package org.infinispan.persistence;

/**
 * Wraps the key/value to be stored in the cache.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public interface KeyValueWrapper<K, V, T> {

   T wrap(K key, V value);

   V unwrap(T object);

}
