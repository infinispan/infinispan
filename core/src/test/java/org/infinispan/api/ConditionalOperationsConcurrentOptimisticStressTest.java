package org.infinispan.api;

import org.testng.annotations.Test;

import java.util.List;

/**
 * @author Mircea Markus
 * @author William Burns
 * @since 7.0
 */
@Test (groups = "stress", testName = "api.ConditionalOperationsConcurrentOptimisticStressTest")
public class ConditionalOperationsConcurrentOptimisticStressTest extends ConditionalOperationsConcurrentStressTest {

   public ConditionalOperationsConcurrentOptimisticStressTest() {
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

   @Override
   public void testPutIfAbsent() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new PutIfAbsentOperation(false));
   }
}
