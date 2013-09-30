package org.infinispan.query.api;

import org.testng.annotations.Test;

/**
 * Testing non-indexed values on InfinispanDirectory in case of transactional cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.api.TransactionalInfinispanDirectoryNonIndexedValuesTest")
public class TransactionalInfinispanDirectoryNonIndexedValuesTest extends InfinispanDirectoryNonIndexedValuesTest {

   protected boolean isTransactional() {
      return true;
   }
}
