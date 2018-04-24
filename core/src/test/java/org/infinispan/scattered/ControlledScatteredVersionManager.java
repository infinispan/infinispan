package org.infinispan.scattered;

import org.infinispan.scattered.impl.ScatteredVersionManagerImpl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ControlledScatteredVersionManager<K> extends ScatteredVersionManagerImpl<K> {
   AtomicInteger regularCounter = new AtomicInteger();
   AtomicInteger removeCounter = new AtomicInteger();

   public static void flush(boolean expectRemoval, ControlledScatteredVersionManager... svms) {
      int[] regularCounters = new int[svms.length];
      int[] removeCounters = new int[svms.length];
      for (int i = 0; i < svms.length; ++i) {
         regularCounters[i] = svms[i].regularCounter.get();
         removeCounters[i] = svms[i].removeCounter.get();
         if (!svms[i].startFlush()) {
            // did not start anything
            regularCounters[i] = Integer.MIN_VALUE;
            removeCounters[i] = Integer.MIN_VALUE;
         }
      }
      for (;;) {
         boolean done = true;
         for (int i = 0; i < svms.length; ++i) {
            int svmRegularCounters = svms[i].regularCounter.get();
            int svmRemoveCounters = svms[i].removeCounter.get();
            if (regularCounters[i] >= svmRegularCounters) {
               log.tracef("SVM %d had %d regular invalidations, now has %d", i, regularCounters[i], svmRegularCounters);
               done = false;
            }
            if (expectRemoval && removeCounters[i] >= svmRemoveCounters) {
               log.tracef("SVM %d had %d remove invalidations, now has %d", i, removeCounters[i], svmRemoveCounters);
               done = false;
            }
         }
         if (done) break;
         LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
      }
   }

   @Override
   protected void regularInvalidationFinished(Object[] keys, int topologyIds[], long[] versions, boolean[] isRemoved, boolean force) {
      super.regularInvalidationFinished(keys, topologyIds, versions, isRemoved, force);
      regularCounter.incrementAndGet();
   }

   @Override
   protected void removeInvalidationsFinished() {
      super.removeInvalidationsFinished();
      removeCounter.incrementAndGet();
   }
}
