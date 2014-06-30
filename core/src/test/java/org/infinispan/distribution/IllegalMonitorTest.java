package org.infinispan.distribution;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicInteger;


/**
 * See ISPN-919 : It's possible we try to release a lock we didn't acquire.
 * This is by design, so that we don't have to keep track of them:
 * @see org.infinispan.util.concurrent.locks.LockManager#possiblyLocked(org.infinispan.container.entries.CacheEntry) 
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 5.0
 */
@Test(groups = "functional", testName = IllegalMonitorTest.TEST_NAME)
public class IllegalMonitorTest extends BaseDistFunctionalTest<Object, String> {

   protected static final String TEST_NAME = "distribution.IllegalMonitorTest";
   private static final AtomicInteger sequencer = new AtomicInteger();
   private final String key = TEST_NAME;

   public IllegalMonitorTest() {
      sync = true;
      tx = false;
      testRetVals = true;
      l1CacheEnabled = true;
   }

   /**
    * This test would throw many IllegalMonitorStateException if they where not hidden by the
    * implementation of the LockManager
    * 
    * @throws InterruptedException
    */
   @Test(threadPoolSize = 7, invocationCount = 21)
   public void testScenario() throws InterruptedException {
      TestResourceTracker.backgroundTestStarted(this);
      int myId = sequencer.incrementAndGet();
      AdvancedCache cache = this.caches.get(myId % this.INIT_CLUSTER_SIZE).getAdvancedCache();
      for (int i = 0; i < 100; i++) {
         if (i % 4 == 0)
            cache.withFlags(Flag.SKIP_LOCKING).put(key, "value");
         cache.withFlags(Flag.SKIP_LOCKING).remove(key);
      }
      cache.clear();
   }
}
