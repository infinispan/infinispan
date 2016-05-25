package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.Test;

/**
 * Verifies the atomic semantic of Infinispan's implementations of java.util.concurrent.ConcurrentMap'
 * conditional operations.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2012 Red Hat Inc.
 * @author William Burns
 * @see java.util.concurrent.ConcurrentMap#replace(Object, Object, Object)
 * @since 7.0
 */
@Test(groups = "stress", testName = "api.ConditionalOperationsConcurrentStressTest")
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.SCATTERED_SYNC})
public class ConditionalOperationsConcurrentStressTest extends ConditionalOperationsConcurrentTest {
   public ConditionalOperationsConcurrentStressTest() {
      super(3, 500, 4);
   }
}
