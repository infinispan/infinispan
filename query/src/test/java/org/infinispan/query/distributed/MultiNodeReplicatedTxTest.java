package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Similiar as MultiNodeReplicatedTest, but uses transactional configuration for the Infinispan.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MultiNodeReplicatedTxTest")
public class MultiNodeReplicatedTxTest extends MultiNodeReplicatedTest {

   protected boolean transactionsEnabled() {
      return true;
   }

   @Override
   protected String getConfigurationResourceName() {
      return "dynamic-transactional-indexing-replication.xml";
   }


}
