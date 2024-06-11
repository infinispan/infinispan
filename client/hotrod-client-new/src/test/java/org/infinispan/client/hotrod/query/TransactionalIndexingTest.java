package org.infinispan.client.hotrod.query;

import org.testng.annotations.Test;

/**
 * @since 9.4
 */
@Test(testName = "client.hotrod.query.TransactionalIndexingTest", groups = "functional")
public class TransactionalIndexingTest extends MultiHotRodServerQueryTest {

   @Override
   protected boolean useTransactions() {
      return true;
   }
}
