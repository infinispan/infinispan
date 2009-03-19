package org.horizon.remoting.transport;

import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This is an abstraction of a cluster-wide synchronization.  Its purpose is to maintain a set of locks that are aware
 * of block and unblock commands issued across a cluster.  In addition to these block and unblock phases, sub-phases
 * such as a start processing
 *
 * @author Manik Surtani
 * @since 1.0
 */
@ThreadSafe
public interface DistributedSync {

   /**
    * @return the number of syncs that have occured
    */
   int getSyncCount();

   /**
    * Blocks until the start of a sync - either by the current RPCManager instance or a remote one - is received.  This
    * should return immediately if sync is already in progress.
    *
    * @param timeout  timeout after which to give up
    * @param timeUnit time unit
    * @throws TimeoutException if waiting for the sync times out.
    */
   void blockUntilAcquired(long timeout, TimeUnit timeUnit) throws TimeoutException;

   /**
    * Blocks until an ongoing sync ends.  This is returns immediately if there is no ongoing sync.
    *
    * @param timeout  timeout after which to give up
    * @param timeUnit time unit
    * @throws TimeoutException if waiting for an ongoing sync to end times out.
    */
   void blockUntilReleased(long timeout, TimeUnit timeUnit) throws TimeoutException;

   /**
    * Acquires the sync.  This could be from a local or remote source.
    */
   void acquireSync();

   /**
    * Releases the sync.  This could be from a local or remote source.
    */
   void releaseSync();

   /**
    * Acquires a processing lock.  This is typically acquired after the sync is acquired, and is meant for local (not
    * remote) use.
    *
    * @param exclusive whether the lock is exclusive
    * @param timeout   timeout after which to give up
    * @param timeUnit  time unit
    * @throws TimeoutException if waiting for the lock times out
    */
   void acquireProcessingLock(boolean exclusive, long timeout, TimeUnit timeUnit) throws TimeoutException;

   /**
    * Releases any processing locks that may be held by the current thread.
    */
   void releaseProcessingLock();
}
