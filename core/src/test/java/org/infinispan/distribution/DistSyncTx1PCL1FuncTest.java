package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.distribution.L1TxInterceptor;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.testng.AssertJUnit.*;

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
}