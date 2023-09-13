package org.infinispan.server.functional.hotrod;

/**
 * A key and value generator for Hot Rod testing.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public interface KeyValueGenerator<K, V> {

   K key(int index);

   V value(int index);

   void assertEquals(V expected, V actual);
}
