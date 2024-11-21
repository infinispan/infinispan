package org.infinispan.client.hotrod;

import org.infinispan.counter.api.CounterManager;

/**
 * A {@link CounterManager} factory for Hot Rod client.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public final class RemoteCounterManagerFactory {

   private RemoteCounterManagerFactory() {
   }

   public static CounterManager asCounterManager(RemoteCacheManager cacheManager) {
      return cacheManager.getCounterManager();
   }

}
