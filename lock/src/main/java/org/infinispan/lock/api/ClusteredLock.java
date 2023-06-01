package org.infinispan.lock.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.lock.exception.ClusteredLockException;

/**
 * ClusteredLock is a data structure used for concurrent programming between Infinispan instances in cluster mode.
 *
 * A typical usage idiom for {@link ClusteredLock#lock} will be :
 *
 * {@code
 *
 *  ClusteredLock lock = clm.get("lock");
 *  lock.lock()
 *  .thenRun(() ->
 *          try {
 *            // manipulate protected state
 *          } finally {
 *              return lock.unlock();
 *          }
 *  )
 * }
 *
 * A typical usage idiom for {@link ClusteredLock#tryLock} will be :
 *
 * {@code
 *
 *  lock.tryLock()
 *    .thenCompose(result -> {
 *       if (result) {
 *        try {
 *            // manipulate protected state
 *            } finally {
 *               return lock.unlock();
 *            }
 *       } else {
 *          // Do something else
 *       }
 *    });
 * }
 *
 * @author Katia Aresti, karesti@redhat.com
 * @see <a href="http://infinispan.org/documentation/">Infinispan documentation</a>
 * @since 9.2
 */
public interface ClusteredLock {

   /**
    * Acquires the lock. If the lock is not available then the {@link CompletableFuture} waits until the lock has been acquired.
    * Currently, there is no maximum time specified for a lock request to fail, so this could cause thread starvation.
    *
    * @return a completed {@link CompletableFuture} when the lock is acquired
    * @throws ClusteredLockException when the lock does not exist
    */
   CompletableFuture<Void> lock();

   /**
    * Acquires the lock only if it is free at the time of invocation.
    * Acquires the lock if it is available and returns immediately with with the {@link CompletableFuture} holding the value {@code true}.
    * If the lock is not available then this method will return immediately with the {@link CompletableFuture} holding the value {@code false}.
    *
    * @return {@code CompletableFuture(true)} if the lock was acquired and {@code CompletableFuture(false)} otherwise
    * @throws ClusteredLockException when the lock does not exist
    */
   CompletableFuture<Boolean> tryLock();

   /**
    * If the lock is available this method returns immediately with the {@link CompletableFuture} holding the value {@code true}.
    * If the lock is not available then the {@link CompletableFuture} waits until :
    * <ul>
    *    <li>The lock is acquired</li>
    *    <li>The specified waiting time elapses</li>
    * </ul>
    *
    * If the lock is acquired then the {@link CompletableFuture} will complete with the value {@code true}.
    * If the specified waiting time elapses then the {@link CompletableFuture} will complete with the value {@code false}.
    * If the time is less than or equal to zero, the method will not wait at all.
    *
    * @param time, the maximum time to wait for the lock
    * @param unit, the time unit of the {@code time} argument
    * @return {@code CompletableFuture(true)} if the lock was acquired and {@code CompletableFuture(false)} if the waiting time elapsed before the lock was acquired
    * @throws ClusteredLockException when the lock does not exist
    */
   CompletableFuture<Boolean> tryLock(long time, TimeUnit unit);

   /**
    * Releases the lock. Only the holder of the lock may release the lock.
    *
    * @return a completed {@link CompletableFuture} when the lock is released
    * @throws ClusteredLockException when the lock does not exist
    */
   CompletableFuture<Void> unlock();

   /**
    * Returns a {@link CompletableFuture<Boolean>} holding {@code true} when the lock is locked and {@code false} when the lock is released.
    *
    * @return a {@link CompletableFuture} holding a {@link Boolean}
    * @throws ClusteredLockException when the lock does not exist
    */
   CompletableFuture<Boolean> isLocked();

   /**
    * Returns a {@link CompletableFuture<Boolean>} holding {@code true} when the lock is owned by the caller and
    * {@code false} when the lock is owned by someone else or it's released.
    *
    * @return a {@link CompletableFuture} holding a {@link Boolean}
    * @throws ClusteredLockException when the lock does not exist
    */
   CompletableFuture<Boolean> isLockedByMe();
}
