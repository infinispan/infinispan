package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.configuration.MultimapConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncMultimaps {
   <K, V> CompletionStage<AsyncMultimap<K, V>> create(String name, MultimapConfiguration cacheConfiguration);

   <K, V> CompletionStage<AsyncMultimap<K, V>> create(String name, String template);

   <K, V> CompletionStage<AsyncMultimap<K, V>> get(String name);

   CompletionStage<Void> remove(String name);

   Flow.Publisher<String> names();

   CompletionStage<Void> createTemplate(String name, MultimapConfiguration cacheConfiguration);

   CompletionStage<Void> removeTemplate(String name);

   Flow.Publisher<String> templateNames();
}
