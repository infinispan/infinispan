package org.infinispan.remoting.transport.jgroups;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.remoting.transport.DistributedSync;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static org.infinispan.util.Util.prettyPrintTime;

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
   private final ReclosableLatch joinInProgress = new ReclosableLatch(false);
   private static final Log log = LogFactory.getLog(JGroupsDistSync.class);
   public static final boolean trace = log.isTraceEnabled();


   public SyncResponse blockUntilAcquired(long timeout, TimeUnit timeUnit) throws TimeoutException {
      int initState = flushWaitGateCount.get();
      try {
         if (!flushWaitGate.await(timeout, timeUnit))
            throw new TimeoutException("Timed out waiting for a cluster-wide sync to be acquired. (timeout = " + prettyPrintTime(timeout) + ")");
      } catch (InterruptedException ie) {
         currentThread().interrupt();
      }

      return initState == flushWaitGateCount.get() ? SyncResponse.STATE_PREEXISTED : SyncResponse.STATE_ACHIEVED;

   }

   public SyncResponse blockUntilReleased(long timeout, TimeUnit timeUnit) throws TimeoutException {
      int initState = flushBlockGateCount.get();
      try {
         if (!flushBlockGate.await(timeout, timeUnit))
            throw new TimeoutException("Timed out waiting for a cluster-wide sync to be released. (timeout = " + prettyPrintTime(timeout) + ")");
      } catch (InterruptedException ie) {
         currentThread().interrupt();
      }


      return initState == flushWaitGateCount.get() ? SyncResponse.STATE_PREEXISTED : SyncResponse.STATE_ACHIEVED;
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
      Lock lock = exclusive ? processingLock.writeLock() : processingLock.readLock();
      try {
         if (!lock.tryLock(timeout, timeUnit))
            throw new TimeoutException(format("%s could not obtain %s processing lock after %s.  Locks in question are %s and %s", currentThread().getName(), exclusive ? "exclusive" : "shared", prettyPrintTime(timeout, timeUnit), processingLock.readLock(), processingLock.writeLock()));
//         log.info("Acquired processing lock xcl = %s.  Locks are %s and %s", exclusive, processingLock.readLock(), processingLock.writeLock());
      } catch (InterruptedException ie) {
         currentThread().interrupt();
      }
   }

   public void releaseProcessingLock(boolean exclusive) {
      try {
         if (exclusive) {
            processingLock.writeLock().unlock();
         } else {
            processingLock.readLock().unlock();
         }
//         log.info("Releasing %s processing lock.  Locks now are %s and %s", exclusive ? "exclusive": "shared", processingLock.readLock(), processingLock.writeLock());
      } catch (IllegalMonitorStateException imse) {
         if (trace) log.trace("Did not own lock!");
      }
   }


   public void signalJoinInProgress() {
      if (trace)
         log.trace("Closing joinInProgress gate");
      joinInProgress.close();
   }
   public void signalJoinCompleted() {
      if (trace)
         log.trace("Releasing " + joinInProgress + " gate");
     joinInProgress.open();
   }

public void blockUntilNoJoinsInProgress() throws InterruptedException {
      joinInProgress.await();
   }
}
