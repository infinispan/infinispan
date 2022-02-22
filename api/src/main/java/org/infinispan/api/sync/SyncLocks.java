package org.infinispan.api.sync;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.configuration.LockConfiguration;

/**
 * @since 14.0
 **/
public interface SyncLocks {
   SyncLock create(String name, LockConfiguration configuration);

   SyncLock get(String name);

   void remove(String name);

   CloseableIterable<String> names();
}
