package org.infinispan.util.concurrent.locks;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.context.impl.TxInvocationContext;

/**
 * A manager that checks and waits for older topology transaction with conflicting keys.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface PendingLockManager {

   /**
    * Check for any transaction with older topology id to complete that may have the lock for any key in {@code keys}
    * acquired.
    * <p>
    * Multiple invocations with the same transaction returns the same {@link PendingLockPromise}.
    *
    * @param ctx  the {@link TxInvocationContext}.
    * @param key  the key to check.
    * @param time timeout.
    * @param unit {@link TimeUnit} of {@code time}.
    * @return a {@link PendingLockPromise}.
    */
   PendingLockPromise checkPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit);

   /**
    * Check for any transaction with older topology id to complete that may have the lock for any key in {@code keys}
    * acquired.
    * <p>
    * Multiple invocations with the same transaction returns the same {@link PendingLockPromise}.
    *
    * @param ctx  the {@link TxInvocationContext}.
    * @param keys the keys to check.
    * @param time timeout.
    * @param unit {@link TimeUnit} of {@code time}.
    * @return a {@link PendingLockPromise}.
    */
   PendingLockPromise checkPendingTransactionsForKeys(TxInvocationContext<?> ctx, Collection<Object> keys, long time, TimeUnit unit);

}
