package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.configuration.MultiMapConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncMultiMap<K, V> {
   /**
    * The name of this multimap
    * @return
    */
   String name();

   /**
    * The configuration of this multimap
    * @return
    */
   CompletionStage<MultiMapConfiguration> configuration();

   /**
    * Return the container of this MultiMap
    * @return
    */
   AsyncContainer container();

   CompletionStage<Void> add(K key, V value);

   Flow.Publisher<V> get(K key);

   CompletionStage<Boolean> remove(K key);

   CompletionStage<Boolean> remove(K key, V value);

   CompletionStage<Boolean> containsKey(K key);

   CompletionStage<Boolean> containsEntry(K key, V value);

   CompletionStage<Long> estimateSize();
}
