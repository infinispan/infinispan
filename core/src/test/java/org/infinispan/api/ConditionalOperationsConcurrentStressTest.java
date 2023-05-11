package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Verifies the atomic semantic of Infinispan's implementations of java.util.concurrent.ConcurrentMap'
 * conditional operations.
 *
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2012 Red Hat Inc.
 * @author William Burns
 * @see java.util.concurrent.ConcurrentMap#replace(Object, Object, Object)
 * @since 7.0
 */
@Test(groups = "stress", testName = "api.ConditionalOperationsConcurrentStressTest", timeOut = 15*60*1000, invocationCount = 1000)
public class ConditionalOperationsConcurrentStressTest extends ConditionalOperationsConcurrentTest {
   @Override
   public Object[] factory() {
      return new Object[] {
            new ConditionalOperationsConcurrentStressTest().cacheMode(CacheMode.DIST_SYNC),
      };
   }

   public ConditionalOperationsConcurrentStressTest() {
      super(3, 500, 4);
   }

   @Override
   public void testReplace() throws Exception {
      super.testReplace();
   }

   @Override
   public void testConditionalRemove() throws Exception {
      super.testConditionalRemove();
   }

   @Override
   public void testPutIfAbsent() throws Exception {
      super.testPutIfAbsent();
   }
}
