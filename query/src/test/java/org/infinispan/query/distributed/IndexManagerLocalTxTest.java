package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Similiar to MultiNodeLocalTest, only uses transactional infinispan configuration.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.IndexManagerLocalTxTest")
public class IndexManagerLocalTxTest extends IndexManagerLocalTest {

   public boolean transactionsEnabled() {
      return true;
   }

}
