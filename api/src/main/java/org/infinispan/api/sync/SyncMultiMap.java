package org.infinispan.api.sync;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.MultiMapConfiguration;

/**
 * @param <K>
 * @param <V>
 * @since 14.0
 */
public interface SyncMultiMap<K, V> {

   String name();

   MultiMapConfiguration configuration();

   /**
    * Return the container of this multimap
    * @return
    */
   SyncContainer container();

   void add(K key, V value);

   CloseableIterable<V> get(K key);

   boolean remove(K key);

   boolean remove(K key, V value);

   boolean containsKey(K key);

   boolean containsEntry(K key, V value);

   long estimateSize();
}
