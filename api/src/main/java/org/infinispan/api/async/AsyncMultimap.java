package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.configuration.MultimapConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncMultimap<K, V> {
   /**
    * The name of this multimap
    *
    * @return
    */
   String name();

   /**
    * The configuration of this multimap
    *
    * @return
    */
   CompletionStage<MultimapConfiguration> configuration();

   /**
    * Return the container of this Multimap
    *
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
