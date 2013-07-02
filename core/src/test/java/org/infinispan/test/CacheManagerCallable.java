package org.infinispan.test;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A task that executes operations against a given cache manager.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class CacheManagerCallable {

   protected final EmbeddedCacheManager cm;

   public CacheManagerCallable(EmbeddedCacheManager cm) {
      this.cm = cm;
   }

   public void call() {
      // No-op
   }

}
