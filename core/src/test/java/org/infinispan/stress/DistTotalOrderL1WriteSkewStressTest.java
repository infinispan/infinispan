package org.infinispan.stress;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "stress", testName = "stress.DistTotalOrderL1WriteSkewStressTest")
public class DistTotalOrderL1WriteSkewStressTest extends DistL1WriteSkewStressTest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      super.decorate(builder);
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .useSynchronization(false)
            .recovery().disable();
   }
}
