package org.infinispan.stress;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "stress", testName = "stress.DistTotalOrderWriteSkewStressTest")
public class DistTotalOrderWriteSkewStressTest extends DistWriteSkewStressTest {

   @Override
   protected void decorate(ConfigurationBuilder builder) {
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
            .useSynchronization(false)
            .recovery().disable();
   }
}
