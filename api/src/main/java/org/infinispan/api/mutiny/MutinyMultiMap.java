package org.infinispan.api.mutiny;

import org.infinispan.api.configuration.MultiMapConfiguration;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @param <K>
 * @param <V>
 * @since 14.0
 */
public interface MutinyMultiMap<K, V> {

   String name();

   Uni<MultiMapConfiguration> configuration();

   /**
    * Return the container of this MultiMap
    * @return
    */
   MutinyContainer container();

   Uni<Void> add(K key, V value);

   Multi<V> get(K key);

   Uni<Boolean> remove(K key);

   Uni<Boolean> remove(K key, V value);

   Uni<Boolean> containsKey(K key);

   Uni<Boolean> containsEntry(K key, V value);

   Uni<Long> estimateSize();
}
