package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.configuration.CacheConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncCaches {
   <K, V> CompletionStage<AsyncCache<K, V>> create(String name, CacheConfiguration cacheConfiguration);

   <K, V> CompletionStage<AsyncCache<K, V>> create(String name, String template);

   <K, V> CompletionStage<AsyncCache<K, V>> get(String name);

   CompletionStage<Void> remove(String name);

   Flow.Publisher<String> names();

   <K, V> CompletionStage<Void> createTemplate(String name, CacheConfiguration cacheConfiguration);

   CompletionStage<Void> removeTemplate(String name);

   Flow.Publisher<String> templateNames();
}
