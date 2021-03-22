package org.infinispan.api.mutiny;

import org.infinispan.api.configuration.LockConfiguration;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public interface MutinyLocks {
   Uni<MutinyLock> lock(String name);

   Uni<MutinyLock> create(String name, LockConfiguration configuration);

   Uni<Void> remove(String name);

   Multi<String> names();
}
