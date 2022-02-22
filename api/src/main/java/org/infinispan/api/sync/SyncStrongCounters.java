package org.infinispan.api.sync;

import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
public interface SyncStrongCounters {
   SyncStrongCounter get(String name);

   SyncStrongCounter create(String name, CounterConfiguration counterConfiguration);

   void remove(String name);

   Iterable<String> names();
}
