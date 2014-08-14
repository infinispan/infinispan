package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Similiar to MultiNodeLocalTest, only uses transactional infinispan configuration.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MultiNodeLocalTxTest")
public class MultiNodeLocalTxTest extends MultiNodeLocalTest {

   public boolean transactionsEnabled() {
      return true;
   }

}
