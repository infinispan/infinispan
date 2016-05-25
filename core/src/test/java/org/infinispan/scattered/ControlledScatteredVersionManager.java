package org.infinispan.scattered;

import org.infinispan.scattered.impl.ScatteredVersionManagerImpl;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ControlledScatteredVersionManager<K> extends ScatteredVersionManagerImpl<K> {
   AtomicInteger regularCounter = new AtomicInteger();
   AtomicInteger removeCounter = new AtomicInteger();

   @Override
   protected void regularInvalidationFinished(Object[] keys, long[] versions, boolean[] isRemoved, boolean force) {
      super.regularInvalidationFinished(keys, versions, isRemoved, force);
      regularCounter.incrementAndGet();
   }

   @Override
   protected void removeInvalidationsFinished() {
      super.removeInvalidationsFinished();
      removeCounter.incrementAndGet();
   }
}
