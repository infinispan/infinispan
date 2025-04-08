package org.infinispan.hibernate.cache.v62.impl;

import java.util.concurrent.CompletableFuture;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.access.PutFromLoadValidator;

/**
 * Delegate for non-transactional invalidation caches
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
class NonTxInvalidationCacheAccessDelegate extends org.infinispan.hibernate.cache.commons.access.NonTxInvalidationCacheAccessDelegate {
   public NonTxInvalidationCacheAccessDelegate(InfinispanDataRegion region, PutFromLoadValidator validator) {
      super(region, validator);
   }

   @Override
   protected void registerLocalInvalidation(Object session, Object lockOwner, Object key) {
      Sync sync = (Sync) ((SharedSessionContractImplementor) session).getCacheTransactionSynchronization();
      if (log.isTraceEnabled()) {
         log.tracef("Registering synchronization on transaction in %s, cache %s: %s", lockOwner, cache.getName(), key);
      }
      sync.registerAfterCommit(new LocalInvalidationInvocation(putValidator, key, lockOwner));
   }

   @Override
   protected void registerClusteredInvalidation(Object session, Object lockOwner, Object key) {
      Sync sync = (Sync) ((SharedSessionContractImplementor) session).getCacheTransactionSynchronization();
      if (log.isTraceEnabled()) {
         log.tracef("Registering synchronization on transaction in %s, cache %s: %s", lockOwner, cache.getName(), key);
      }
      sync.registerAfterCommit(new InvalidationInvocation(nonTxPutFromLoadInterceptor, key, lockOwner));
   }

   @Override
   protected void invoke(Object session, InvocationContext ctx, RemoveCommand command) {
      CompletableFuture<Object> future = invoker.invokeAsync(ctx, command);
      // LockingInterceptor removes this flag when the command is applied and we only wait for RPC
      if (!command.hasAnyFlag(FlagBitSets.FORCE_WRITE_LOCK)) {
         Sync sync = (Sync) ((SharedSessionContractImplementor) session).getCacheTransactionSynchronization();
         sync.registerBeforeCommit(future);
      } else {
         log.trace("Removal was not applied immediately, waiting.");
         future.join();
      }
   }
}
