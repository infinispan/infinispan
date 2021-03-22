package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.configuration.CounterConfiguration;

import io.smallrye.mutiny.Multi;

/**
 * @since 14.0
 **/
public interface AsyncStrongCounters {
   CompletionStage<AsyncStrongCounter> get(String name);

   CompletionStage<AsyncStrongCounter> create(String name, CounterConfiguration configuration);

   CompletionStage<Void> remove(String name);

   Multi<String> names();
}
