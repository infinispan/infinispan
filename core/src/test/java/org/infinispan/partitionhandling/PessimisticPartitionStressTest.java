package org.infinispan.partitionhandling;

import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "partitionhandling.PessimisticPartitionStressTest")
public class PessimisticPartitionStressTest extends NonTxPartitionStressTest {

   public PessimisticPartitionStressTest() {
      transactionMode = TransactionMode.TRANSACTIONAL;
      lockingMode = LockingMode.PESSIMISTIC;
   }
}
