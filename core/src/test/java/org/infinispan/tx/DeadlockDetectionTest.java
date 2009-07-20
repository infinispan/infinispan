package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockAssert;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.interceptors.DeadlockDetectingInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.manager.CacheManager;
import org.infinispan.remoting.ReplicationException;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.statetransfer.StateTransferException;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.locks.DeadlockDetectingLockManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import javax.transaction.RollbackException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Functional test for deadlock detection.
 *
 * @author Mircea.Markus@jboss.com
 *         <p/>
 *         TODO - test for deadlock on invalidation
 *          TODO - add test deadlock with distribution
 */
@Test(testName = "tx.DeadlockDetectionTest", groups = "functional")
public class DeadlockDetectionTest extends MultipleCacheManagersTest {

   private ControlledRpcManager controlledRpcManager1;
   private ControlledRpcManager controlledRpcManager2;
   private CountDownLatch replicationLatch;
   private ExecutorThread t1;
   private ExecutorThread t2;
   private DeadlockDetectingLockManager ddLm1;
   private DeadlockDetectingLockManager ddLm2;


   protected void createCacheManagers() throws Throwable {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC);
      config.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
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
      t1 = new ExecutorThread(cache(0, "test"), 1);
      t2 = new ExecutorThread(cache(1, "test"), 2);
      replicationLatch = new CountDownLatch(1);
      controlledRpcManager1.setReplicationLatch(replicationLatch);
      controlledRpcManager2.setReplicationLatch(replicationLatch);
      log.trace("_________________________ Here is beggins");
   }

   @AfterMethod
   public void afterMethod() {
      t1.stopThread();
      t2.stopThread();
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(0, "test"))).resetStatistics();
      ((DeadlockDetectingLockManager) TestingUtil.extractLockManager(cache(1, "test"))).resetStatistics();
   }

   public void testDeadlockDetectionAndAsyncCaches() {
      Configuration config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_ASYNC);
      config.setEnableDeadlockDetection(true);
      config.setUseLockStriping(false);
      CacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
      cm.defineCache("test", config);
      try {
         cm.getCache("test");
         assert false : "Exception expected";
      } catch (ConfigurationException e) {
         //expected
         System.out.println("Error message is " + e.getMessage());
      }
      cm.stop();
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
      assert OperationsResult.BEGGIN_TX_OK == t1.execute(Operations.BEGGIN_TX);
      assert OperationsResult.BEGGIN_TX_OK == t2.execute(Operations.BEGGIN_TX);
      System.out.println("After beggin");

      t1.execute(Operations.PUT_KEY_VALUE);
      t2.execute(Operations.PUT_KEY_VALUE);
      System.out.println("After put key value");

      t1.clearResponse();
      t2.clearResponse();

      t1.executeNoResponse(Operations.COMMIT_TX);
      t2.executeNoResponse(Operations.COMMIT_TX);

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

      assert ddLm1.getDetectedDeadlocks() + ddLm2.getDetectedDeadlocks() >= 1;

      LockManager lm1 = TestingUtil.extractComponent(cache(0, "test"), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");
      LockManager lm2 = TestingUtil.extractComponent(cache(1, "test"), LockManager.class);
      assert !lm2.isLocked("key") : "It is locked by " + lm2.getOwner("key");
      LockAssert.assertNoLocks(cache(0, "test"));
   }

   public void testLocalVsLocalTxDeadlock() {
      CacheManager cm = null;
      try {
         cm = TestCacheManagerFactory.createLocalCacheManager();
         Configuration configuration = new Configuration();
         configuration.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());
         configuration.setEnableDeadlockDetection(true);
         configuration.setUseLockStriping(false);
         configuration.setExposeJmxStatistics(true);
         cm.defineCache("test", configuration);
         Cache localCache = cm.getCache("test");
         DeadlockDetectingLockManager lockManager = (DeadlockDetectingLockManager) TestingUtil.extractLockManager(localCache);

         ExecutorThread t1 = new ExecutorThread(localCache, 0);
         ExecutorThread t2 = new ExecutorThread(localCache, 1);


         assert OperationsResult.BEGGIN_TX_OK == t1.execute(Operations.BEGGIN_TX);
         assert OperationsResult.BEGGIN_TX_OK == t2.execute(Operations.BEGGIN_TX);
         System.out.println("After beggin");

         t1.setKeyValue("k1", "value_1_t1");
         t2.setKeyValue("k2", "value_2_t2");

         assert OperationsResult.PUT_KEY_VALUE_OK == t1.execute(Operations.PUT_KEY_VALUE);
         assert OperationsResult.PUT_KEY_VALUE_OK == t2.execute(Operations.PUT_KEY_VALUE);

         System.out.println("After first PUT");
         assert lockManager.isLocked("k1");
         assert lockManager.isLocked("k2");


         t1.setKeyValue("k2", "value_2_t1");
         t2.setKeyValue("k1", "value_1_t2");
         t1.executeNoResponse(Operations.PUT_KEY_VALUE);
         t2.executeNoResponse(Operations.PUT_KEY_VALUE);

         Object response1 = t1.waitForResponse();
         Object response2 = t2.waitForResponse();

         assert xor(response1 instanceof DeadlockDetectedException, response2 instanceof DeadlockDetectedException) : "expected one and only one exception: " + response1 + ", " + response2;
         assert xor(response1 == OperationsResult.PUT_KEY_VALUE_OK, response2 == OperationsResult.PUT_KEY_VALUE_OK) : "expected one and only one exception: " + response1 + ", " + response2;

         assert lockManager.isLocked("k1");
         assert lockManager.isLocked("k2");
         assert lockManager.getOwner("k1") == lockManager.getOwner("k2");

         if (response1 instanceof Exception) {
            assert OperationsResult.COMMIT_TX_OK == t2.execute(Operations.COMMIT_TX);
            assertEquals("value_1_t2", localCache.get("k1"));
            assertEquals("value_2_t2", localCache.get("k2"));
            assert t1.execute(Operations.COMMIT_TX) instanceof RollbackException;
         } else {
            assert OperationsResult.COMMIT_TX_OK == t1.execute(Operations.COMMIT_TX);
            assertEquals("value_1_t1", localCache.get("k1"));
            assertEquals("value_2_t1", localCache.get("k2"));
            assert t2.execute(Operations.COMMIT_TX) instanceof RollbackException;
         }
         assert lockManager.getNumberOfLocksHeld() == 0;
         assertEquals(lockManager.getDetectedDeadlocks(), 1);
      } finally {
         TestingUtil.killCacheManagers(cm);
      }
   }


   public void testDeadlockDetectedOneTx() throws Exception {
      t1.setKeyValue("key", "value1");

      LockManager lm2 = TestingUtil.extractComponent(cache(1, "test"), LockManager.class);
      NonTxInvocationContext ctx = cache(1, "test").getAdvancedCache().getInvocationContextContainer().createNonTxInvocationContext();
      lm2.lockAndRecord("key", ctx);
      assert lm2.isLocked("key");


      assert OperationsResult.BEGGIN_TX_OK == t1.execute(Operations.BEGGIN_TX) : "but received " + t1.lastResponse();
      t1.execute(Operations.PUT_KEY_VALUE);

      t1.clearResponse();
      t1.executeNoResponse(Operations.COMMIT_TX);

      replicationLatch.countDown();
      System.out.println("Now replication is triggered");

      t1.waitForResponse();


      Object t1CommitRsp = t1.lastResponse();

      assert t1CommitRsp instanceof Exception : "expected exception, received " + t1.lastResponse();

      LockManager lm1 = TestingUtil.extractComponent(cache(0, "test"), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");

      lm2.unlock("key", ctx.getLockOwner());
      assert !lm2.isLocked("key");
      assert !lm1.isLocked("key");
   }

   public void testLockReleasedWhileTryingToAcquire() throws Exception {
      t1.setKeyValue("key", "value1");

      LockManager lm2 = TestingUtil.extractComponent(cache(1, "test"), LockManager.class);
      NonTxInvocationContext ctx = cache(1, "test").getAdvancedCache().getInvocationContextContainer().createNonTxInvocationContext();
      lm2.lockAndRecord("key", ctx);
      assert lm2.isLocked("key");


      assert OperationsResult.BEGGIN_TX_OK == t1.execute(Operations.BEGGIN_TX) : "but received " + t1.lastResponse();
      t1.execute(Operations.PUT_KEY_VALUE);

      t1.clearResponse();
      t1.executeNoResponse(Operations.COMMIT_TX);

      replicationLatch.countDown();

      Thread.sleep(3000); //just to make sure the remote tx thread managed to spin around for some times. 
      lm2.unlock("key", ctx.getLockOwner());

      t1.waitForResponse();


      Object t1CommitRsp = t1.lastResponse();

      assert t1CommitRsp == OperationsResult.COMMIT_TX_OK : "expected true, received " + t1.lastResponse();

      LockManager lm1 = TestingUtil.extractComponent(cache(0, "test"), LockManager.class);
      assert !lm1.isLocked("key") : "It is locked by " + lm1.getOwner("key");

      assert !lm2.isLocked("key");
      assert !lm1.isLocked("key");
   }

   public static enum Operations {
      BEGGIN_TX, COMMIT_TX, PUT_KEY_VALUE, STOP_THREAD
   }

   public static enum OperationsResult {
      BEGGIN_TX_OK, COMMIT_TX_OK, PUT_KEY_VALUE_OK, STOP_THREAD_OK
   }

   public static final class ExecutorThread extends Thread {

      private static Log log = LogFactory.getLog(ExecutorThread.class);

      private Cache<Object, Object> cache;
      private BlockingQueue<Object> toExecute = new ArrayBlockingQueue<Object>(1);
      private volatile Object response;
      private CountDownLatch responseLatch = new CountDownLatch(1);

      private volatile Object key, value;

      public void setKeyValue(Object key, Object value) {
         this.key = key;
         this.value = value;
      }

      public ExecutorThread(Cache<Object, Object> cache, int index) {
         super("ExecutorThread-" + index);
         this.cache = cache;
         start();
      }

      public Object execute(Operations op) {
         try {
            responseLatch = new CountDownLatch(1);
            toExecute.put(op);
            responseLatch.await();
            return response;
         } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected", e);
         }
      }

      public void executeNoResponse(Operations op) {
         try {
            responseLatch = null;
            response = null;
            toExecute.put(op);
         } catch (InterruptedException e) {
            throw new RuntimeException("Unexpected", e);
         }
      }

      @Override
      public void run() {
         Operations operation;
         boolean run = true;
         while (run) {
            try {
               operation = (Operations) toExecute.take();
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
            System.out.println("about to process operation " + operation);
            switch (operation) {
               case BEGGIN_TX: {
                  TransactionManager txManager = TestingUtil.getTransactionManager(cache);
                  try {
                     txManager.begin();
                     setResponse(OperationsResult.BEGGIN_TX_OK);
                  } catch (Exception e) {
                     log.trace("Failure on beggining tx", e);
                     setResponse(e);
                  }
                  break;
               }
               case COMMIT_TX: {
                  TransactionManager txManager = TestingUtil.getTransactionManager(cache);
                  try {
                     txManager.commit();
                     setResponse(OperationsResult.COMMIT_TX_OK);
                  } catch (Exception e) {
                     log.trace("Exception while committing tx", e);
                     setResponse(e);
                  }
                  break;
               }
               case PUT_KEY_VALUE: {
                  try {
                     cache.put(key, value);
                     log.trace("Successfully exucuted putKeyValue(" + key + ", " + value + ")");
                     setResponse(OperationsResult.PUT_KEY_VALUE_OK);
                  } catch (Exception e) {
                     log.trace("Exception while executing putKeyValue(" + key + ", " + value + ")", e);
                     setResponse(e);
                  }
                  break;
               }
               case STOP_THREAD: {
                  System.out.println("Exiting...");
                  toExecute = null;
                  run = false;
                  break;
               }
            }
            if (responseLatch != null) responseLatch.countDown();
         }
         setResponse("EXIT");
      }

      private void setResponse(Object e) {
         log.trace("setResponse to " + e);
         response = e;
      }

      public void stopThread() {
         execute(Operations.STOP_THREAD);
         while (!this.getState().equals(State.TERMINATED)) {
            try {
               Thread.sleep(50);
            } catch (InterruptedException e) {
               throw new IllegalStateException(e);
            }
         }
      }

      public Object lastResponse() {
         return response;
      }

      public void clearResponse() {
         response = null;
      }

      public Object waitForResponse() {
         while (response == null) {
            try {
               Thread.sleep(50);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }
         return response;
      }
   }

   private boolean xor(boolean b1, boolean b2) {
      return (b1 || b2) && !(b1 && b2);
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

      public List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue, ResponseFilter responseFilter) {
         return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue, responseFilter);
      }

      public List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout, boolean usePriorityQueue) {
         return realOne.invokeRemotely(recipients, rpcCommand, mode, timeout, usePriorityQueue);
      }

      public List<Response> invokeRemotely(List<Address> recipients, ReplicableCommand rpcCommand, ResponseMode mode, long timeout) throws Exception {
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

      public void invokeRemotely(List<Address> recipients, ReplicableCommand rpc, boolean sync) throws ReplicationException {
         realOne.invokeRemotely(recipients, rpc, sync);
      }

      public void invokeRemotely(List<Address> recipients, ReplicableCommand rpc, boolean sync, boolean usePriorityQueue) throws ReplicationException {
         realOne.invokeRemotely(recipients, rpc, sync, usePriorityQueue);
      }

      public void invokeRemotelyInFuture(List<Address> recipients, ReplicableCommand rpc, NotifyingNotifiableFuture<Object> future) {
         realOne.invokeRemotelyInFuture(recipients, rpc, future);
      }

      public void invokeRemotelyInFuture(List<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future) {
         realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future);
      }

      public void invokeRemotelyInFuture(List<Address> recipients, ReplicableCommand rpc, boolean usePriorityQueue, NotifyingNotifiableFuture<Object> future, long timeout) {
         realOne.invokeRemotelyInFuture(recipients, rpc, usePriorityQueue, future, timeout);
      }

      public Transport getTransport() {
         return realOne.getTransport();
      }

      public Address getCurrentStateTransferSource() {
         return realOne.getCurrentStateTransferSource();
      }
   }
}
