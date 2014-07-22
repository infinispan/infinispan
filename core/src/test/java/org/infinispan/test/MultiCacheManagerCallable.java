package org.infinispan.test;

import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A task that executes operations against a group of cache managers.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class MultiCacheManagerCallable {

   protected final EmbeddedCacheManager[] cms;

   public MultiCacheManagerCallable(EmbeddedCacheManager... cms) {
      this.cms = cms;
   }

   public void call() throws Exception {
      // No-op
   }

}
