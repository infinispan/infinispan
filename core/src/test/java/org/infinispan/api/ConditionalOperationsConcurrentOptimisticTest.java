package org.infinispan.api;

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "api.ConditionalOperationsConcurrentOptimisticTest")
public class ConditionalOperationsConcurrentOptimisticTest extends ConditionalOperationsConcurrentTest {

   public ConditionalOperationsConcurrentOptimisticTest() {
      cacheMode = CacheMode.DIST_SYNC;
      transactional = true;
   }

   @Override
   public void testReplace() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ReplaceOperation(false));
   }

   @Override
   public void testConditionalRemove() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ConditionalRemoveOperation(false));
   }

   public void testPutIfAbsent() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new PutIfAbsentOperation(false));
   }
}
