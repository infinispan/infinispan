package org.infinispan.distribution;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


@Test(groups = "functional", testName = "distribution.DistSyncTx1PCL1FuncTest")
public class DistSyncTx1PCL1FuncTest extends DistSyncTxL1FuncTest {
   public DistSyncTx1PCL1FuncTest() {
      super();
      onePhaseCommitOptimization = true;
   }

   @Override
   protected Class<? extends VisitableCommand> getCommitCommand() {
      return PrepareCommand.class;
   }

   @Test(groups = "unstable")
   @Override
   public void testBackupOwnerInvalidatesL1WhenPrimaryIsUnaware() throws InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      super.testBackupOwnerInvalidatesL1WhenPrimaryIsUnaware();
   }
}
