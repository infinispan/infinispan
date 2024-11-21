package org.infinispan.client.hotrod.test;

import org.infinispan.client.hotrod.RemoteCacheManager;

/**
 * A task that executes operations against a given remote cache manager.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class RemoteCacheManagerCallable {

   protected final RemoteCacheManager rcm;

   public RemoteCacheManagerCallable(RemoteCacheManager rcm) {
      this.rcm = rcm;
   }

   public void call() {
      // No-op
   }

}
