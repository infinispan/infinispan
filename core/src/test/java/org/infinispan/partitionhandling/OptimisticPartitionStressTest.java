package org.infinispan.partitionhandling;

import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "partitionhandling.OptimisticPartitionStressTest")
public class OptimisticPartitionStressTest extends NonTxPartitionStressTest {

   public OptimisticPartitionStressTest() {
      transactionMode = TransactionMode.TRANSACTIONAL;
      lockingMode = LockingMode.OPTIMISTIC;
   }
}
