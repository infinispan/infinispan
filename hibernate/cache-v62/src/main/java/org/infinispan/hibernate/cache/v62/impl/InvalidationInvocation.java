package org.infinispan.hibernate.cache.v62.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.hibernate.cache.commons.access.NonTxPutFromLoadInterceptor;
import org.infinispan.hibernate.cache.commons.util.InfinispanMessageLogger;

/**
 * Synchronization that should release the locks after invalidation is complete.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InvalidationInvocation implements Invocation {
   private static final InfinispanMessageLogger log = InfinispanMessageLogger.Provider.getLog(InvalidationInvocation.class);

   private final Object lockOwner;
   private final NonTxPutFromLoadInterceptor nonTxPutFromLoadInterceptor;
   private final Object key;

   public InvalidationInvocation(NonTxPutFromLoadInterceptor nonTxPutFromLoadInterceptor, Object key, Object lockOwner) {
      assert lockOwner != null;
      this.nonTxPutFromLoadInterceptor = nonTxPutFromLoadInterceptor;
      this.key = key;
      this.lockOwner = lockOwner;
   }

   @Override
   public CompletableFuture<Void> invoke(boolean success) {
      if (log.isTraceEnabled()) {
         log.tracef("After completion callback, success=%b", success);
      }
      nonTxPutFromLoadInterceptor.endInvalidating(key, lockOwner, success);
      return null;
   }
}
