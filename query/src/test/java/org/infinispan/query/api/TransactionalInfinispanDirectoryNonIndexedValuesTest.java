package org.infinispan.query.api;

import org.testng.annotations.Test;

/**
 * Testing non-indexed values on InfinispanDirectory in case of transactional cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.api.TransactionalInfinispanDirectoryNonIndexedValuesTest", enabled = false,
      description = "Enable when the ISPN-2815 is fixed.")
public class TransactionalInfinispanDirectoryNonIndexedValuesTest extends InfinispanDirectoryNonIndexedValuesTest {

   protected boolean isTransactional() {
      return true;
   }

   @Test(enabled = false, description = "Enable when the ISPN-2815 is fixed.")
   public void testReplaceSimpleSearchable() {

   }
}
