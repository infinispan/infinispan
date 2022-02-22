package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncWeakCounters {
   CompletionStage<AsyncWeakCounter> get(String name);

   CompletionStage<AsyncWeakCounter> create(String name, CounterConfiguration configuration);

   CompletionStage<Void> remove(String name);

   Flow.Publisher<String> names();
}
