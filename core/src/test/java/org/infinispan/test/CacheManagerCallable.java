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
   private final boolean clear;

   public CacheManagerCallable(EmbeddedCacheManager cm) {
      this(cm, false);
   }

   public CacheManagerCallable(EmbeddedCacheManager cm, boolean clear) {
      this.cm = cm;
      this.clear = clear;
   }

   public void call() throws Exception {
      // No-op
   }

   public final boolean clearBeforeKill() {
      return clear;
   }

}
