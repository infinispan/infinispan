package org.infinispan.stress;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "stress", testName = "stress.TotalOrderWriteSkewStressTest")
public class TotalOrderWriteSkewStressTest extends ReplWriteSkewStressTest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .useSynchronization(false)
            .recovery().disable();
   }
}

