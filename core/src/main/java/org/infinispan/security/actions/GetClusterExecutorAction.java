package org.infinispan.security.actions;

import java.security.PrivilegedAction;

import org.infinispan.Cache;
import org.infinispan.manager.ClusterExecutor;

/**
 * GetClusterExecutorAction.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GetClusterExecutorAction implements PrivilegedAction<ClusterExecutor> {

   private final Cache<?, ?> cache;

   public GetClusterExecutorAction(Cache<?, ?> cache) {
      this.cache = cache;
   }

   @Override
   public ClusterExecutor run() {
      return cache.getCacheManager().executor();
   }

}
