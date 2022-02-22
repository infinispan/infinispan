package org.infinispan.api.mutiny;

import org.infinispan.api.configuration.CounterConfiguration;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public interface MutinyWeakCounters {
   Uni<MutinyWeakCounter> get(String name);

   Uni<MutinyWeakCounter> create(String name, CounterConfiguration configuration);

   Uni<Void> remove(String name);

   Multi<String> names();
}
