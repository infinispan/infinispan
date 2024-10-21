package org.infinispan.util.concurrent.locks.deadlock;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.KeyAwareLockPromise;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

@CleanupAfterMethod
@Test(groups = "functional", testName = "deadlock.DistributedDeadlockDetectionTest")
public class DistributedDeadlockDetectionTest extends AbstractDeadlockTest {

   @Override
   protected int clusterSize() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
   }

   public void testTransactionsSucceed() throws Throwable {
      AdvancedCache<String, String> c0 = this.<String, String>cache(0).getAdvancedCache();
      AdvancedCache<String, String> c1 = this.<String, String>cache(1).getAdvancedCache();

      TransactionManager tm0 = c0.getTransactionManager();
      TransactionManager tm1 = c1.getTransactionManager();

      CyclicBarrier barrier = new CyclicBarrier(3);
      Future<Void> f0 = fork(() -> {
         barrier.await(10, TimeUnit.SECONDS);
         tm0.begin();
         c0.put("key-0", "value");
         tm0.commit();
      });

      Future<Void> f1 = fork(() -> {
         barrier.await(10, TimeUnit.SECONDS);
         tm1.begin();
         c1.put("key-1", "value");
         tm1.commit();
      });

      barrier.await(10, TimeUnit.SECONDS);
      f0.get(10, TimeUnit.SECONDS);
      f1.get(10, TimeUnit.SECONDS);

      assertThat(c0.get("key-0")).isEqualTo("value");
      assertThat(c1.get("key-1")).isEqualTo("value");
   }

   public void testSmallDeadlockCycle() throws Throwable {
      DeadlockClusterHandler cluster = createDeadlock(0, 1);

      cluster.lockCommandCheckPoint(0).proceedLockCommand(cluster.address(1), cluster.transaction(1));
      cluster.lockCommandCheckPoint(1).proceedLockCommand(cluster.address(0), cluster.transaction(0));

      // Assert none of the transactions are complete.
      // Assert the lock owners on each node follows the dependency graph.
      cluster.assertTransactionsNotCompleted(0, 1);

      // Allow commands to proceed, should trigger deadlock detection.
      cluster.releaseAllLockCommands(0, 1);

      // Should trigger deadlock commands!
      cluster.deadlockCheckpoint(0).awaitStartDeadlockCommand(cluster.transaction(1), cluster.transaction(0));

      // Allow all requests to proceed.
      // After the release, the algorithm should proceed and eventually one of the transaction completes.
      cluster.releaseAllDeadlockCommands();
      cluster.assertOperationsFinish(true);
   }

   public void testResolveDeadlock() throws Throwable {
      Map<Integer, Integer> dependency = new HashMap<>();
      dependency.put(0, 1);
      dependency.put(1, 2);
      dependency.put(2, 0);
      DeadlockClusterHandler cluster = createDeadlock(dependency, 0, 1, 2);

      // Assert none of the transactions are complete.
      // Assert the lock owners on each node follows the dependency graph.
      cluster.assertTransactionsNotCompleted(0, 1, 2);
      for (Map.Entry<Integer, Integer> entry : dependency.entrySet()) {
         cluster.assertLockOwner(entry.getKey(), entry.getValue());
      }

      // Allow commands to proceed, should trigger deadlock detection.
      cluster.releaseAllLockCommands(0, 1, 2);

      // Should trigger deadlock commands!
      // Deadlock commands utilize an ordering mechanism, every place the owner is older than the initiator we have a probe.
      // In this case, we'll have two probe commands. The newest transaction has ID=3.
      // The probe command is sent from node 0, when L=3 sees L=2 (from node 1) is holding the lock.
      cluster.deadlockCheckpoint(1).awaitStartDeadlockCommand(cluster.transaction(2), cluster.transaction(1));

      // Allow all requests to proceed.
      // After the release, the algorithm should proceed and eventually one of the transaction completes.
      cluster.releaseAllDeadlockCommands();
      cluster.assertOperationsFinish(true);
   }

   public void testDeadlockResolveWithHigherTxInPlace() throws Throwable {
      DeadlockClusterHandler cluster = createDeadlock(0, 1, 2);

      cluster.assertTransactionsNotCompleted(0, 1, 2);

      // The oldest tx is in node 0. Therefore, we'll add a newest tx in the table.
      GlobalTransaction newestGtx = new GlobalTransaction(address(0), false);
      LockManager lockManager = TestingUtil.extractLockManager(cache(0));
      KeyAwareLockPromise promise = lockManager.lock(cluster.getKeyOwned(0), newestGtx, 10, TimeUnit.SECONDS);
      assertThat(promise.isAvailable()).isTrue();

      // Create the deadlock scenario.
      Map<Integer, Integer> dependency = new HashMap<>();
      dependency.put(0, 1);
      dependency.put(1, 2);
      dependency.put(2, 0);
      cluster.allowLockCommands(dependency);
      cluster.assertTransactionsNotCompleted(0, 1, 2);

      // Verify in nodes 1 and 2 the lock owners follow the dependency.
      // In node 0 we have the newestGtx as the owner.
      assertThat(lockManager.getOwner(cluster.getKeyOwned(0))).isEqualTo(newestGtx);
      cluster.assertLockOwner(1, 2);
      cluster.assertLockOwner(2, 0);

      // Release the lock commands to trigger the deadlock detection.
      cluster.releaseAllLockCommands(0, 1, 2);

      // Since we have one of the nodes with a newer tx, we'll only have 1 deadlock command.
      // This deadlock command is from N2 to N0, when Tx2 sees Tx1 holding the lock.
      cluster.deadlockCheckpoint(0).awaitStartDeadlockCommand(cluster.transaction(1), cluster.transaction(0));

      // Let allow all deadlock commands to proceed. However, this would not complete the transactions, since there is still
      // another transaction holding the lock on N0.
      cluster.releaseAllDeadlockCommands();
      cluster.assertTransactionsNotCompleted(0, 1, 2);

      // Then, we release the lock with the newest TX. This will trigger all the remaining messages to complete the tx.
      lockManager.unlock(cluster.getKeyOwned(0), newestGtx);
      cluster.assertOperationsFinish(false);
   }

   public void testNodeJoinsDuringDeadlock() throws Throwable {
      Map<Integer, Integer> dependency = new HashMap<>();
      dependency.put(0, 1);
      dependency.put(1, 2);
      dependency.put(2, 0);
      DeadlockClusterHandler cluster = createDeadlock(dependency, 0, 1, 2);

      // Assert none of the transactions are complete.
      // Assert the lock owners on each node follows the dependency graph.
      cluster.assertTransactionsNotCompleted(0, 1, 2);
      for (Map.Entry<Integer, Integer> entry : dependency.entrySet()) {
         cluster.assertLockOwner(entry.getKey(), entry.getValue());
      }

      // Allow commands to proceed, should trigger deadlock detection.
      cluster.releaseAllLockCommands(0, 1, 2);

      // Should trigger deadlock commands!
      cluster.deadlockCheckpoint(1).awaitStartDeadlockCommand(cluster.transaction(2), cluster.transaction(1));
      cluster.assertTransactionsNotCompleted(0, 1, 2);

      // Add a new member to the cluster.
      addClusterEnabledCacheManager(testCacheConfiguration());
      waitForClusterToForm();

      // Release all the deadlock commands and ensure it completes successfully even with topology changes.
      cluster.releaseAllDeadlockCommands();
      cluster.assertOperationsFinish(false);
   }

   public void testNodeJoinsDuringLockAcquisition() throws Throwable {
      Map<Integer, Integer> dependency = new HashMap<>();
      dependency.put(0, 1);
      dependency.put(1, 2);
      dependency.put(2, 0);
      DeadlockClusterHandler cluster = createDeadlock(dependency, 0, 1, 2);

      // Assert the transactions are incomplete and who owns the locks.
      cluster.assertTransactionsNotCompleted(0, 1, 2);
      for (Map.Entry<Integer, Integer> entry : dependency.entrySet()) {
         cluster.assertLockOwner(entry.getKey(), entry.getValue());
      }

      // Mid lock acquisition, a new node joins the cluster.
      addClusterEnabledCacheManager(testCacheConfiguration());
      waitForClusterToForm();

      // Release the lock commands. This should trigger any retries in the lock commands and the deadlock algorithm.
      cluster.releaseAllLockCommands(0, 1, 2);

      // Wait a deadlock probe command to make sure.
      cluster.waitForDeadlockCommand();

      // Let the algorithm proceed and the operations should complete.
      cluster.releaseAllDeadlockCommands();
      cluster.assertOperationsFinish(true);
   }

   public void testNodeLeavesDuringLockAcquisition() throws Throwable {
      // Add new member before deadlocking.
      addClusterEnabledCacheManager(testCacheConfiguration());
      waitForClusterToForm();

      Map<Integer, Integer> dependency = new HashMap<>();
      dependency.put(0, 1);
      dependency.put(1, 2);
      dependency.put(2, 0);
      DeadlockClusterHandler cluster = createDeadlock(dependency, 0, 1, 2);

      // Assert the transactions are incomplete and who owns the locks.
      cluster.assertTransactionsNotCompleted(0, 1, 2);
      for (Map.Entry<Integer, Integer> entry : dependency.entrySet()) {
         cluster.assertLockOwner(entry.getKey(), entry.getValue());
      }

      // Mid lock acquisition, a node leaves the cluster.
      TestingUtil.killCacheManagers(cacheManagers.remove(3));
      waitForClusterToForm();

      // Release the lock commands. This should trigger any retries in the lock commands and the deadlock algorithm.
      cluster.releaseAllLockCommands(0, 1, 2);

      // Wait a deadlock probe command to make sure.
      cluster.waitForDeadlockCommand();

      // Let the algorithm proceed and the operations should complete.
      cluster.releaseAllDeadlockCommands();
      cluster.assertOperationsFinish(true);
   }

   public void testParticipantLeavesLockAcquisition() throws Throwable {
      // Add new member before deadlocking.
      addClusterEnabledCacheManager(testCacheConfiguration());
      waitForClusterToForm();

      Map<Integer, Integer> dependency = new HashMap<>();
      dependency.put(0, 1);
      dependency.put(1, 2);
      dependency.put(2, 3);
      dependency.put(3, 0);
      DeadlockClusterHandler cluster = createDeadlock(dependency, 0, 1, 2, 3);

      // Assert the transactions are incomplete and who owns the locks.
      cluster.assertTransactionsNotCompleted(0, 1, 2, 3);
      for (Map.Entry<Integer, Integer> entry : dependency.entrySet()) {
         cluster.assertLockOwner(entry.getKey(), entry.getValue());
      }

      // Mid lock acquisition, a node leaves the cluster.
      TestingUtil.killCacheManagers(cacheManagers.remove(3));
      waitForClusterToForm();

      // Release the lock commands. This should trigger any retries in the lock commands and the deadlock algorithm.
      cluster.releaseAllLockCommands(0, 1, 2);

      // Wait a deadlock probe command to make sure.
      cluster.waitForDeadlockCommand();

      // Let the algorithm proceed and the operations should complete.
      cluster.releaseAllDeadlockCommands();
      cluster.assertOperationsFinish(false);
   }

   public void testParticipantLeavesDuringDeadlockResolution() throws Throwable {
      // Add new member before deadlocking.
      addClusterEnabledCacheManager(testCacheConfiguration());
      waitForClusterToForm();

      Map<Integer, Integer> dependency = new HashMap<>();
      dependency.put(0, 1);
      dependency.put(1, 2);
      dependency.put(2, 3);
      dependency.put(3, 0);
      DeadlockClusterHandler cluster = createDeadlock(dependency, 0, 1, 2, 3);

      // Assert the transactions are incomplete and who owns the locks.
      cluster.assertTransactionsNotCompleted(0, 1, 2, 3);
      for (Map.Entry<Integer, Integer> entry : dependency.entrySet()) {
         cluster.assertLockOwner(entry.getKey(), entry.getValue());
      }

      // Allow commands to proceed, should trigger deadlock detection.
      cluster.releaseAllLockCommands(0, 1, 2, 3);

      // Should trigger deadlock commands!
      cluster.waitForDeadlockCommand();
      cluster.assertTransactionsNotCompleted(0, 1, 2, 3);

      // Add a new member to the cluster.
      TestingUtil.killCacheManagers(cacheManagers.remove(3));
      waitForClusterToForm();

      // Release all the deadlock commands and ensure it completes successfully even with topology changes.
      cluster.releaseAllDeadlockCommands();

      cluster.assertOperationsFinish(true);
   }
}
