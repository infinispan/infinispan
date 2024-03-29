package org.infinispan.util.concurrent.locks.impl;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.concurrent.locks.PendingLockManager;
import org.infinispan.util.concurrent.locks.PendingLockPromise;

/**
 * An {@link PendingLockManager} implementation that does nothing.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class NoOpPendingLockManager implements PendingLockManager {

   private NoOpPendingLockManager() {
   }

   public static NoOpPendingLockManager getInstance() {
      return Wrapper.INSTANCE;
   }

   @Override
   public PendingLockPromise checkPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit) {
      return PendingLockPromise.NO_OP;
   }

   @Override
   public PendingLockPromise checkPendingTransactionsForKeys(TxInvocationContext<?> ctx, Collection<Object> keys, long time, TimeUnit unit) {
      return PendingLockPromise.NO_OP;
   }

   private static class Wrapper {
      private static final NoOpPendingLockManager INSTANCE = new NoOpPendingLockManager();
   }
}
