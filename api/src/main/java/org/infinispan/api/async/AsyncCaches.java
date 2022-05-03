package org.infinispan.api.async;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.api.configuration.CacheConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncCaches {
   <K, V> CompletionStage<AsyncCache<K, V>> create(String name, CacheConfiguration cacheConfiguration);

   <K, V> CompletionStage<AsyncCache<K, V>> create(String name, String template);

   <K, V> CompletionStage<AsyncCache<K, V>> get(String name);

   CompletionStage<Void> remove(String name);

   CompletionStage<Set<String>> names();

   CompletionStage<Void> createTemplate(String name, CacheConfiguration cacheConfiguration);

   CompletionStage<Void> removeTemplate(String name);

   CompletionStage<Set<String>> templateNames();
}
