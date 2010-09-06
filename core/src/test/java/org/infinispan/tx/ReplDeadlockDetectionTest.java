package org.infinispan.tx;

import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.interceptors.DeadlockDetectingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.ReplicationException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.PerCacheExecutorThread;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Functional test for deadlock detection.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(testName = "tx.ReplDeadlockDetectionTest", groups = "functional")
public class ReplDeadlockDetectionTest extends MultipleCacheManagersTest {

   protected ControlledRpcManager controlledRpcManager1;
   protected ControlledRpcManager controlledRpcManager2;
   protected CountDownLatch replicationLatch;
   protected PerCacheExecutorThread t1;
   protected PerCacheExecutorThread t2;
   protected DeadlockDetectingLockManager ddLm1;
   protected DeadlockDetectingLockManager ddLm2;

   protected Configuration.CacheMode cacheMode = Configuration.CacheMode.REPL_SYNC;

   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(cacheMode, true);
      config.setEnableDeadlockDetection(true);
      config.setSyncCommitPhase(true);
      config.setSyncRollbackPhase(true);
      config.setUseLockStriping(false);
      assert config.isEnableDeadlockDetection();
      createClusteredCaches(2, "test", config);
      assert config.isEnableDeadlockDetection();

      assert cache(0, "test").getConfiguration().isEnableDeadlockDetection();
      assert cache(1, "test").getConfiguration().isEnableDeadlockDetection();
      assert !cache(0, "test").getConfiguration().isExposeJmxStatistics();
      assert !cache(1, "test").getConfiguration().isExposeJmxStatistics();

      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0, "test"))).setExposeJmxStats(true);
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1, "test"))).setExposeJmxStats(true);

      RpcManager rpcManager1 = TestingUtil.extractComponent(cache(0, "test"), RpcManager.class);
      RpcManager rpcManager2 = TestingUtil.extractComponent(cache(1, "test"), RpcManager.class);

      controlledRpcManager1 = new ControlledRpcManager(rpcManager1);
      controlledRpcManager2 = new ControlledRpcManager(rpcManager2);
      TestingUtil.replaceComponent(cache(0, "test"), RpcManager.class, controlledRpcManager1, true);
      TestingUtil.replaceComponent(cache(1, "test"), RpcManager.class, controlledRpcManager2, true);

      assert TestingUtil.extractComponent(cache(0, "test"), RpcManager.class) instanceof ControlledRpcManager;
      assert TestingUtil.extractComponent(cache(1, "test"), RpcManager.class) instanceof ControlledRpcManager;

      ddLm1 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0, "test"));
      ddLm2 = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1, "test"));
   }


   @BeforeMethod
   public void beforeMethod() {
      t1 = new PerCacheExecutorThread(cache(0, "test"), 1);
      t2 = new PerCacheExecutorThread(cache(1, "test"), 2);
      replicationLatch = new CountDownLatch(1);
      controlledRpcManager1.setReplicationLatch(replicationLatch);
      controlledRpcManager2.setReplicationLatch(replicationLatch);
      log.trace("_________________________ Here it begins");
   }

   @AfterMethod
   public void afterMethod() {
      t1.stopThread();
      t2.stopThread();
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0, "test"))).resetStatistics();
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1, "test"))).resetStatistics();
   }
   
   public void testExpectedInnerStructure() {
      LockManager lockManager = TestingUtil.extractComponent(cache(0, "test"), LockManager.class);
      assert lockManager instanceof DeadlockDetectingLockManager;

      InterceptorChain ic = TestingUtil.extractComponent(cache(0, "test"), InterceptorChain.class);
      assert ic.containsInterceptorType(DeadlockDetectingInterceptor.class);
   }

   public void testDeadlockDetectedTwoTransactions() throws Exception {
      t1.setKeyValue("key", "value1");
      t2.setKeyValue("key", "value2");
      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);
      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t2.execute(PerCacheExecutorThread.Operations.BEGGIN_TX);
      System.out.println("After begin");

      t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      t2.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);
      System.out.println("After put key value");

      t1.clearResponse();
      t2.clearResponse();

      t1.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);
      t2.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);

      System.out.println("Now replication is triggered");
      replicationLatch.countDown();


      Object t1Commit = t1.waitForResponse();
      Object t2Commit = t2.waitForResponse();
      System.out.println("After commit: " + t1Commit + ", " + t2Commit);

      assert xor(t1Commit instanceof Exception, t2Commit instanceof Exception) : "only one thread must be failing " + t1Commit + "," + t2Commit;
      System.out.println("t2Commit = " + t2Commit);
      System.out.println("t1Commit = " + t1Commit);

      if (t1Commit instanceof Exception) {
         System.out.println("t1 rolled back");
         Object o = cache(0, "test").get("key");
         assert o != null;
         assert o.equals("value2");
      } else {
         System.out.println("t2 rolled back");
         Object o = cache(0, "test").get("key");
         assert o != null;
         assert o.equals("value1");
         o = cache(1, "test").get("key");
         assert o != null;
         assert o.equals("value1");
      }

      assert ddLm1.getDetectedRemoteDeadlocks() + ddLm2.getDetectedRemoteDeadlocks() >= 1;

      LockManager lm1 = TestingUtil.extractComponent(cache(0, "test"), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");
      LockManager lm2 = TestingUtil.extractComponent(cache(1, "test"), LockManager.class);
      assert !lm2.isLocked("key") : "It is locked by " + lm2.getOwner("key");
      LockAssert.assertNoLocks(cache(0, "test"));
   }

   public void testDeadlockDetectedOneTx() throws Exception {
      t1.setKeyValue("key", "value1");

      LockManager lm2 = TestingUtil.extractComponent(cache(1, "test"), LockManager.class);
      NonTxInvocationContext ctx = cache(1, "test").getAdvancedCache().getInvocationContextContainer().createNonTxInvocationContext();
      lm2.lockAndRecord("key", ctx);
      assert lm2.isLocked("key");


      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX) : "but received " + t1.lastResponse();
      t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);

      t1.clearResponse();
      t1.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);

      replicationLatch.countDown();
      System.out.println("Now replication is triggered");

      t1.waitForResponse();


      Object t1CommitRsp = t1.lastResponse();

      assert t1CommitRsp instanceof Exception : "expected exception, received " + t1.lastResponse();

      LockManager lm1 = TestingUtil.extractComponent(cache(0, "test"), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");

      lm2.unlock("key");
      assert !lm2.isLocked("key");
      assert !lm1.isLocked("key");
   }

   public void testLockReleasedWhileTryingToAcquire() throws Exception {
      t1.setKeyValue("key", "value1");

      LockManager lm2 = TestingUtil.extractComponent(cache(1, "test"), LockManager.class);
      NonTxInvocationContext ctx = cache(1, "test").getAdvancedCache().getInvocationContextContainer().createNonTxInvocationContext();
      lm2.lockAndRecord("key", ctx);
      assert lm2.isLocked("key");


      assert PerCacheExecutorThread.OperationsResult.BEGGIN_TX_OK == t1.execute(PerCacheExecutorThread.Operations.BEGGIN_TX) : "but received " + t1.lastResponse();
      t1.execute(PerCacheExecutorThread.Operations.PUT_KEY_VALUE);

      t1.clearResponse();
      t1.executeNoResponse(PerCacheExecutorThread.Operations.COMMIT_TX);

      replicationLatch.countDown();

      Thread.sleep(3000); //just to make sure the remote tx thread managed to spin around for some times.
      lm2.unlock("key");

      t1.waitForResponse();


      Object t1CommitRsp = t1.lastResponse();

      assert t1CommitRsp == PerCacheExecutorThread.OperationsResult.COMMIT_TX_OK : "expected true, received " + t1.lastResponse();

      LockManager lm1 = TestingUtil.extractComponent(cache(0, "test"), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");

      assert !lm2.isLocked("key");
      assert !lm1.isLocked("key");
   }

   public static final class ControlledRpcManager implements RpcManager {

      private volatile CountDownLatch replicationLatch;

      public ControlledRpcManager(RpcManager realOne) {
         this.realOne = realOne;
      }

      private RpcManager realOne;

      public void setReplicationLatch(CountDownLatch replicationLatch) {
         this.replicationLatch = replicationLatch;
      }

      public List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
         return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
      }

      public List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
         return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
      }

      public List<Response> invokeRemotely(Collection<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) throws Exception {
         return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout);
      }

      public void retrieveState(String cacheName, long timeout) throws StateTransferException {
         realOne.retrieveState(cacheName, timeout);
      }

      public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync) throws ReplicationException {
         waitFirst();
         realOne.broadcastRpcCommand(rpc, sync);
      }

      public void broadcastRpcCommand(ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException {
         waitFirst();
         realOne.broadcastRpcCommand(rpc, sync, usePriorityQueue);
      }

      private void waitFirst() {
         System.out.println(Thread.currentThread().getName() + " -- replication trigger called!");
         try {
            replicationLatch.await();
         } catch (Exception e) {
            throw new RuntimeException("Unexpected exception!", e);
         }
      }

      public void broadcastRpcCommandInFuture(ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
         realOne.broadcastRpcCommandInFuture(rpc, future);
      }

      public void broadcastRpcCommandInFuture(ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
         realOne.broadcastRpcCommandInFuture(rpc, usePriorityQueue, future);
      }

      public void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync) throws ReplicationException {
         realOne.invokeRemotely(recipients, rpc, sync);
      }

      public void invokeRemotely(Collection<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException {
         realOne.invokeRemotely(recipients, rpc, sync, usePriorityQueue);
      }

      public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
         realOne.invokeRemotelyInFuture(recipients, rpc, future);
      }

      public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
         realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
      }

      public void invokeRemotelyInFuture(Collection<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
         realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
      }

      public Transport getTransport() {
         return realOne.getTransport();
      }

      public Address getCurrentStateTransferSource() {
         return realOne.getCurrentStateTransferSource();
      }

      @Override
      public Address getAddress() {
         return null;
      }
   }
}
