package org.infinispan.util.concurrent.locks.deadlock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "deadlock.PartitionHandlingDeadlockResolutionTest")
public class PartitionHandlingDeadlockResolutionTest extends BasePartitionHandlingTest {

   {
      transactional = true;
      lockingMode = LockingMode.PESSIMISTIC;
   }

   private final DeadlockClusterHandler.TestLeech leech = new DeadlockClusterHandler.TestLeech() {
      @Override
      public <K, V> Cache<K, V> cache(int node) {
         return PartitionHandlingDeadlockResolutionTest.this.cache(node);
      }

      @Override
      public Address address(int node) {
         return PartitionHandlingDeadlockResolutionTest.this.manager(node).getAddress();
      }

      @Override
      public <T> Future<T> fork(Callable<T> callable) {
         return PartitionHandlingDeadlockResolutionTest.this.fork(callable);
      }

      @Override
      public String keyGenerator(String prefix, int node) {
         return PartitionHandlingDeadlockResolutionTest.this.getStringKeyForCache(prefix, cache(node));
      }

      @Override
      public void eventually(DeadlockClusterHandler.ThrowingBooleanSupplier tbs) {
         PartitionHandlingDeadlockResolutionTest.eventually(tbs::getAsBoolean);
      }
   };

   @Override
   protected void customizeCacheConfiguration(ConfigurationBuilder dcc) {
      dcc.transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .deadlockDetection(true);
   }

   private DeadlockClusterHandler clusterHandler(int ... nodes) throws Throwable {
      return DeadlockClusterHandler.create(leech, nodes);
   }

   private DeadlockClusterHandler clusterHandler(Map<Integer, Integer> dependency, int ... nodes) throws Throwable {
      return DeadlockClusterHandler.create(leech, dependency, nodes);
   }

   public void testPartitionAfterLockAcquisition() throws Throwable {
      Map<Integer, Integer> dependency = new HashMap<>();
      dependency.put(0, 1);
      dependency.put(1, 2);
      dependency.put(2, 3);
      dependency.put(3, 0);
      DeadlockClusterHandler dch = clusterHandler(dependency, 0, 1, 2, 3);

      dch.assertTransactionsNotCompleted(0, 1, 2, 3);
      for (Map.Entry<Integer, Integer> entry : dependency.entrySet()) {
         dch.assertLockOwner(entry.getKey(), entry.getValue());
      }

      // Release lock commands to operate. This triggers the deadlock detection.
      dch.releaseAllLockCommands(0, 1, 2, 3);

      // Wait until a deadlock command arrives.
      dch.waitForDeadlockCommand();

      // Now we split the cluster.
      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      partition(0).assertDegradedMode();

      // Release all the deadlock commands.
      dch.releaseAllDeadlockCommands();

      partition(0).merge(partition(1));
      waitForClusterToForm();

      // Wait operations to complete.
      eventually(() -> dch.isTransactionsCompleted(0, 1, 2, 3));
      dch.assertOperationsFinish(false);
   }

   public void testBigAndLittleDeadlockCycles() throws Throwable {
      Map<Integer, Integer> bigDependency = new HashMap<>();
      bigDependency.put(0, 3);
      bigDependency.put(1, 0);
      bigDependency.put(2, 1);
      bigDependency.put(3, 2);
      DeadlockClusterHandler bigDeadlock = clusterHandler(bigDependency, 0, 1, 2, 3);

      bigDeadlock.assertTransactionsNotCompleted(0, 1, 2, 3);
      for (Map.Entry<Integer, Integer> entry : bigDependency.entrySet()) {
         bigDeadlock.assertLockOwner(entry.getKey(), entry.getValue());
      }

      DeadlockClusterHandler littleDeadlock = clusterHandler(0, 1);

      littleDeadlock.releaseAllLockCommands(0, 1);
      bigDeadlock.releaseAllLockCommands(0, 1, 2, 3);

      IntStream.of(2, 3)
            .forEach(node -> {
               bigDeadlock.lockCommandCheckPoint(node).allowLockCommand(littleDeadlock.address(0), littleDeadlock.transaction(0));
               bigDeadlock.lockCommandCheckPoint(node).allowLockCommand(littleDeadlock.address(0), littleDeadlock.transaction(1));
               bigDeadlock.lockCommandCheckPoint(node).allowLockCommand(littleDeadlock.address(1), littleDeadlock.transaction(0));
               bigDeadlock.lockCommandCheckPoint(node).allowLockCommand(littleDeadlock.address(1), littleDeadlock.transaction(1));
            });

      bigDeadlock.waitForDeadlockCommand();
      littleDeadlock.waitForDeadlockCommand();

      splitCluster(new int[]{0, 1}, new int[]{ 2, 3});
      partition(0).assertDegradedMode();

      bigDeadlock.releaseAllDeadlockCommands();
      littleDeadlock.releaseAllDeadlockCommands();

      eventually(() -> littleDeadlock.isTransactionsCompleted(0, 1));
      littleDeadlock.assertOperationsFinish(false);

      partition(0).merge(partition(1));
      waitForClusterToForm();

      eventually(() -> bigDeadlock.isTransactionsCompleted(0, 1, 2, 3));
      bigDeadlock.assertOperationsFinish(false);
   }

   public void testDeadlockResolvedWithinPartitions() throws Throwable {
      DeadlockClusterHandler dch = clusterHandler(0, 1);

      dch.lockCommandCheckPoint(0).proceedLockCommand(dch.address(1), dch.transaction(1));
      dch.lockCommandCheckPoint(1).proceedLockCommand(dch.address(0), dch.transaction(0));
      dch.assertTransactionsNotCompleted(0, 1);

      // Release lock commands to operate. This triggers the deadlock detection.
      dch.releaseAllLockCommands(0, 1);

      // Wait until a deadlock command arrives.
      dch.waitForDeadlockCommand();

      // Now we split the cluster.
      splitCluster(new int[]{0, 1}, new int[]{ 2, 3});
      partition(0).assertDegradedMode();

      // Release all the deadlock commands.
      dch.releaseAllDeadlockCommands();

      // Wait operations to complete.
      dch.assertOperationsFinish(false);

      partition(0).merge(partition(1));
      waitForClusterToForm();
   }
}
