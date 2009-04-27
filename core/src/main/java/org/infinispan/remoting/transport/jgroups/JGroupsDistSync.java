package org.infinispan.remoting.transport.jgroups;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;
import org.infinispan.remoting.transport.DistributedSync;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.ReclosableLatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A DistributedSync based on JGroups' FLUSH protocol
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ThreadSafe
public class JGroupsDistSync implements DistributedSync {

   private final ReentrantReadWriteLock processingLock = new ReentrantReadWriteLock();
   private final ReclosableLatch flushBlockGate = new ReclosableLatch(true);
   private final AtomicInteger flushBlockGateCount = new AtomicInteger(0);
   private final AtomicInteger flushWaitGateCount = new AtomicInteger(0);
   private final ReclosableLatch flushWaitGate = new ReclosableLatch(false);
   private static final Log log = LogFactory.getLog(JGroupsDistSync.class);

   public SyncResponse blockUntilAcquired(long timeout, TimeUnit timeUnit) throws TimeoutException {
      int initState = flushWaitGateCount.get();
      while (true) {
         try {
            if (!flushWaitGate.await(timeout, timeUnit))
               throw new TimeoutException("Timed out waiting for a cluster-wide sync to be acquired. (timeout = " + Util.prettyPrintTime(timeout) + ")");

            return initState == flushWaitGateCount.get() ? SyncResponse.STATE_PREEXISTED : SyncResponse.STATE_ACHIEVED;
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
      }
   }

   public SyncResponse blockUntilReleased(long timeout, TimeUnit timeUnit) throws TimeoutException {
      int initState = flushBlockGateCount.get();
      while (true) {
         try {
            if (!flushBlockGate.await(timeout, timeUnit))
               throw new TimeoutException("Timed out waiting for a cluster-wide sync to be released. (timeout = " + Util.prettyPrintTime(timeout) + ")");

            return initState == flushWaitGateCount.get() ? SyncResponse.STATE_PREEXISTED : SyncResponse.STATE_ACHIEVED;
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
      }
   }

   public void acquireSync() {
      flushBlockGate.close();
      flushWaitGateCount.incrementAndGet();
      flushWaitGate.open();
   }

   public void releaseSync() {
      flushWaitGate.close();
      flushBlockGateCount.incrementAndGet();
      flushBlockGate.open();
   }

   public void acquireProcessingLock(boolean exclusive, long timeout, TimeUnit timeUnit) throws TimeoutException {
      while (true) {
         try {
            Lock lock = exclusive ? processingLock.writeLock() : processingLock.readLock();
            if (!lock.tryLock(timeout, timeUnit))
               throw new TimeoutException("Could not obtain " + (exclusive ? "exclusive" : "shared") + " processing lock");
            break;
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
         }
      }
   }

   public void releaseProcessingLock() {
      try {
         if (processingLock.isWriteLockedByCurrentThread()) {
            processingLock.writeLock().unlock();
         } else {
            processingLock.readLock().unlock();
         }
      } catch (IllegalMonitorStateException imse) {
         if (log.isTraceEnabled()) log.trace("Did not own lock!");
      }
   }
}
