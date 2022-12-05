package org.infinispan.security.actions;

import java.util.Objects;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * GetClusterExecutorAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetClusterExecutorAction implements Supplier<ClusterExecutor> {

   private final Cache<?, ?> cache;
   private final EmbeddedCacheManager cacheManager;

   public GetClusterExecutorAction(Cache<?, ?> cache) {
      this.cache = Objects.requireNonNull(cache);
      this.cacheManager = null;
   }

   public GetClusterExecutorAction(EmbeddedCacheManager cacheManager) {
      this.cache = null;
      this.cacheManager = Objects.requireNonNull(cacheManager);
   }

   @Override
   public ClusterExecutor get() {
      EmbeddedCacheManager manager;
      if (cache != null) {
         manager = cache.getCacheManager();
      } else {
         manager = this.cacheManager;
      }
      return manager.executor();
   }

}
