package org.infinispan.util.concurrent.locks;

import org.infinispan.context.impl.TxInvocationContext;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * A manager that checks and waits for older topology transaction with conflicting keys.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public interface PendingLockManager {

   /**
    * Same as {@link #awaitPendingTransactionsForKey(TxInvocationContext, Object, long, TimeUnit)} but non-blocking.
    * <p>
    * Multiple invocations with the same transaction returns the same {@link PendingLockPromise}. For cleanup purposes,
    * {@link #awaitPendingTransactionsForKey(TxInvocationContext, Object, long, TimeUnit)} must be invoked
    * afterwards.
    *
    * @param ctx  the {@link TxInvocationContext}.
    * @param key  the key to check.
    * @param time timeout.
    * @param unit {@link TimeUnit} of {@code time}.
    * @return a {@link PendingLockPromise}.
    */
   PendingLockPromise checkPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit);

   /**
    * Same as {@link #awaitPendingTransactionsForAllKeys(TxInvocationContext, Collection, long, TimeUnit)} but
    * non-blocking.
    * <p>
    * Multiple invocations with the same transaction returns the same {@link PendingLockPromise}. For cleanup purposes,
    * {@link #awaitPendingTransactionsForAllKeys(TxInvocationContext, Collection, long, TimeUnit)} must be invoked
    * afterwards.
    *
    * @param ctx  the {@link TxInvocationContext}.
    * @param keys the keys to check.
    * @param time timeout.
    * @param unit {@link TimeUnit} of {@code time}.
    * @return a {@link PendingLockPromise}.
    */
   PendingLockPromise checkPendingTransactionsForKeys(TxInvocationContext<?> ctx, Collection<Object> keys, long time, TimeUnit unit);

   /**
    * It waits for any transaction with older topology id to complete that may have the lock for {@code key} acquired.
    *
    * @param ctx  the {@link TxInvocationContext}.
    * @param key  the key to check.
    * @param time timeout.
    * @param unit {@link TimeUnit} of {@code time}.
    * @return the remaining timeout.
    * @throws InterruptedException if the thread is interrupted while waiting.
    */
   long awaitPendingTransactionsForKey(TxInvocationContext<?> ctx, Object key, long time, TimeUnit unit)
         throws InterruptedException;

   /**
    * It waits for any transaction with older topology id to complete that may have the lock for any key in {@code keys}
    * acquired.
    *
    * @param ctx  the {@link TxInvocationContext}.
    * @param keys the keys to check.
    * @param time timeout.
    * @param unit {@link TimeUnit} of {@code time}.
    * @return the remaining timeout.
    * @throws InterruptedException if the thread is interrupted while waiting.
    */
   long awaitPendingTransactionsForAllKeys(TxInvocationContext<?> ctx, Collection<Object> keys, long time, TimeUnit unit)
         throws InterruptedException;
}
