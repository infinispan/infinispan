package org.infinispan.api;

import java.util.List;

import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.2
 */
@Test (groups = "functional", testName = "api.ConditionalOperationsConcurrentWriteSkewTest")
public class ConditionalOperationsConcurrentWriteSkewTest extends ConditionalOperationsConcurrentTest {

   public ConditionalOperationsConcurrentWriteSkewTest() {
      transactional = true;
      writeSkewCheck = true;
   }

   @Override
   public void testReplace() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ReplaceOperation(true));
   }

   @Override
   public void testConditionalRemove() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new ConditionalRemoveOperation(true));
   }

   public void testPutIfAbsent() throws Exception {
      List caches = caches(null);
      testOnCaches(caches, new PutIfAbsentOperation(true));
   }
}
