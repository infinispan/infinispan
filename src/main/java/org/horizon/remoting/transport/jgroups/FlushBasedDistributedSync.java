package org.horizon.remoting.transport.jgroups;

import net.jcip.annotations.ThreadSafe;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.remoting.transport.DistributedSync;
import org.horizon.util.Util;
import org.horizon.util.concurrent.ReclosableLatch;

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
public class FlushBasedDistributedSync implements DistributedSync {

   private final ReentrantReadWriteLock processingLock = new ReentrantReadWriteLock();
   private final ReclosableLatch flushBlockGate = new ReclosableLatch();
   private final AtomicInteger flushCompletionCount = new AtomicInteger();
   private final ReclosableLatch flushWaitGate = new ReclosableLatch(false);
   private static final Log log = LogFactory.getLog(FlushBasedDistributedSync.class);

   public int getSyncCount() {
      return flushCompletionCount.get();
   }

   public void blockUntilAcquired(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
      if (!flushWaitGate.await(timeout, timeUnit)) throw new TimeoutException("Timed out waiting for a cluster-wide sync to be acquired. (timeout = " + Util.prettyPrintTime(timeout) + ")");
   }

   public void blockUntilReleased(long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
      if (!flushBlockGate.await(timeout, timeUnit)) throw new TimeoutException("Timed out waiting for a cluster-wide sync to be released. (timeout = " + Util.prettyPrintTime(timeout) + ")");
   }

   public void acquireSync() {
      flushBlockGate.close();
      flushWaitGate.open();
   }

   public void releaseSync() {
      flushWaitGate.close();
      flushCompletionCount.incrementAndGet();
      flushBlockGate.open();
   }

   public void acquireProcessingLock(boolean exclusive, long timeout, TimeUnit timeUnit) throws TimeoutException, InterruptedException {
      Lock lock = exclusive ? processingLock.writeLock() : processingLock.readLock();
      if (!lock.tryLock(timeout, timeUnit)) throw new TimeoutException("Could not obtain " + (exclusive ? "exclusive" : "shared") + " processing lock");
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
