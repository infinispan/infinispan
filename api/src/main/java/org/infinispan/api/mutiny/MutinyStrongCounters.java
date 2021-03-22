package org.infinispan.api.mutiny;

import org.infinispan.api.configuration.CounterConfiguration;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public interface MutinyStrongCounters {
   Uni<MutinyStrongCounter> get(String name);

   Uni<MutinyStrongCounter> create(String name, CounterConfiguration configuration);

   Uni<Void> remove(String name);

   Multi<String> names();
}
