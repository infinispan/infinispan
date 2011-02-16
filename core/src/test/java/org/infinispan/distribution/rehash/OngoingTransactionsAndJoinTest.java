package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.interceptors.DistTxInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.InboundInvocationHandlerImpl;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;

/**
 * This tests the following scenario:
 * <p/>
 * 1 node exists.  Transactions running.  Some complete, some in prepare, some in commit. New node joins, rehash occurs.
 * Test that the new node is the owner and receives this state.
 */
@Test(groups = "functional", testName = "distribution.rehash.OngoingTransactionsAndJoinTest")
@CleanupAfterMethod
public class OngoingTransactionsAndJoinTest extends MultipleCacheManagersTest {
   Configuration configuration;

   @Override
   protected void createCacheManagers() throws Throwable {
      configuration = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      configuration.setLockAcquisitionTimeout(60000);
      configuration.setUseLockStriping(false);
      addClusterEnabledCacheManager(configuration, true);
   }

   private void injectListeningHandler(CacheContainer ecm, ListeningHandler lh) {
      replaceComponent(ecm, InboundInvocationHandler.class, lh, true);
      JGroupsTransport t = (JGroupsTransport) extractComponent(cache(0), Transport.class);
      CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
      Field f = null;
      try {
         f = card.getClass().getDeclaredField("inboundInvocationHandler");
         f.setAccessible(true);
         f.set(card, lh);
      } catch (NoSuchFieldException e) {
         e.printStackTrace();
      } catch (IllegalAccessException e) {
         e.printStackTrace();
      }
   }

   public void testRehashOnJoin() throws InterruptedException {
      Cache<Object, Object> firstNode = cache(0);
      final CountDownLatch txsStarted = new CountDownLatch(3), txsReady = new CountDownLatch(3), joinEnded = new CountDownLatch(1), rehashStarted = new CountDownLatch(1);
      ListeningHandler listeningHandler = new ListeningHandler(extractComponent(firstNode, InboundInvocationHandler.class), txsReady, joinEnded, rehashStarted);
      injectListeningHandler(firstNode.getCacheManager(), listeningHandler);

      assert firstNode.getAdvancedCache().getComponentRegistry().getComponent(InboundInvocationHandler.class) instanceof ListeningHandler;

      for (int i = 0; i < 10; i++) firstNode.put("OLD" + i, "value");

      UnpreparedDuringRehashTask ut = new UnpreparedDuringRehashTask(firstNode, txsStarted, txsReady, joinEnded, rehashStarted);
      PrepareDuringRehashTask pt = new PrepareDuringRehashTask(firstNode, txsStarted, txsReady, joinEnded, rehashStarted);
      CommitDuringRehashTask ct = new CommitDuringRehashTask(firstNode, txsStarted, txsReady, joinEnded, rehashStarted);

      InterceptorChain ic = TestingUtil.extractComponent(firstNode, InterceptorChain.class);
      ic.addInterceptorAfter(pt, DistTxInterceptor.class);
      ic.addInterceptorAfter(ct, DistTxInterceptor.class);


      Set<Thread> threads = new HashSet<Thread>();
      threads.add(new Thread(ut, "UnpreparedDuringRehashTask"));
      threads.add(new Thread(pt, "PrepareDuringRehashTask"));
      threads.add(new Thread(ct, "CommitDuringRehashTask"));

      for (Thread t : threads) t.start();

      txsStarted.await();
      // start a new node!
      addClusterEnabledCacheManager(configuration, true);
      Cache<?, ?> joiner = cache(1);

      for (Thread t : threads) t.join();

      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(1));

      for (int i = 0; i < 10; i++) {
         Object key = "OLD" + i;
         Object value = joiner.get(key);
         log.info(" TEST: Key %s is %s", key, value);
         assert "value".equals(value) : "Couldn't see key " + key + " on joiner!";
      }

      for (Object key: Arrays.asList(ut.key(), pt.key(), ct.key())) {
         Object value = joiner.get(key);
         log.info(" TEST: Key %s is %s", key, value);
         assert "value".equals(value) : "Couldn't see key " + key + " on joiner!";
      }
   }

   abstract class TransactionalTask extends CommandInterceptor implements Runnable {
      Cache<Object, Object> cache;
      CountDownLatch txsStarted, txsReady, joinEnded, rehashStarted;
      volatile Transaction tx;

      protected void startTx() throws Exception {
         tm(cache).begin();
         cache.put(key(), "value");
         tx = tm(cache).getTransaction();
         txsStarted.countDown();
      }

      abstract Object key();
   }

   class UnpreparedDuringRehashTask extends TransactionalTask {

      UnpreparedDuringRehashTask(Cache<Object, Object> cache, CountDownLatch txsStarted, CountDownLatch txsReady, CountDownLatch joinEnded, CountDownLatch rehashStarted) {
         this.cache = cache;
         this.txsStarted = txsStarted;
         this.txsReady = txsReady;
         this.joinEnded = joinEnded;
         this.rehashStarted = rehashStarted;
      }

      Object key() {
         return "unprepared_during_rehash";
      }

      @Override
      public void run() {
         try {
            // start a tx
            startTx();
            txsReady.countDown();
            joinEnded.await();
            tm(cache).commit();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
   }

   class PrepareDuringRehashTask extends TransactionalTask {

      PrepareDuringRehashTask(Cache<Object, Object> cache, CountDownLatch txsStarted, CountDownLatch txsReady, CountDownLatch joinEnded, CountDownLatch rehashStarted) {
         this.cache = cache;
         this.txsStarted = txsStarted;
         this.txsReady = txsReady;
         this.joinEnded = joinEnded;
         this.rehashStarted = rehashStarted;
      }

      Object key() {
         return "prepare_during_rehash";
      }

      @Override
      public void run() {
         try {
            startTx();
            tm(cache).commit();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext tcx, PrepareCommand cc) throws Throwable {
         if (tcx.getTransaction().equals(tx)) {
            txsReady.countDown();
            rehashStarted.await();
         }
         return super.visitPrepareCommand(tcx, cc);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext tcx, CommitCommand cc) throws Throwable {
         if (tcx.getTransaction().equals(tx)) {
            try {
               joinEnded.await();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }
         return super.visitCommitCommand(tcx, cc);
      }
   }

   class CommitDuringRehashTask extends TransactionalTask {

      CommitDuringRehashTask(Cache<Object, Object> cache, CountDownLatch txsStarted, CountDownLatch txsReady, CountDownLatch joinEnded, CountDownLatch rehashStarted) {
         this.cache = cache;
         this.txsStarted = txsStarted;
         this.txsReady = txsReady;
         this.joinEnded = joinEnded;
         this.rehashStarted = rehashStarted;
      }

      Object key() {
         return "commit_during_rehash";
      }

      @Override
      public void run() {
         try {
            startTx();
            tm(cache).commit();
         } catch (Exception e) {
            throw new RuntimeException(e);
         }

      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext tcx, PrepareCommand cc) throws Throwable {
         Object o = super.visitPrepareCommand(tcx, cc);
         if (tcx.getTransaction().equals(tx)) {
            txsReady.countDown();
         }
         return o;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext tcx, CommitCommand cc) throws Throwable {
         if (tcx.getTransaction().equals(tx)) {
            try {
               rehashStarted.await();
            } catch (InterruptedException e) {
               e.printStackTrace();
            }
         }

         return super.visitCommitCommand(tcx, cc);
      }
   }

   class ListeningHandler extends InboundInvocationHandlerImpl {
      final InboundInvocationHandler delegate;
      final CountDownLatch txsReady, joinEnded, rehashStarted;

      public ListeningHandler(InboundInvocationHandler delegate, CountDownLatch txsReady, CountDownLatch joinEnded, CountDownLatch rehashStarted) {
         this.delegate = delegate;
         this.txsReady = txsReady;
         this.joinEnded = joinEnded;
         this.rehashStarted = rehashStarted;
      }

      @Override
      public Response handle(CacheRpcCommand cmd) throws Throwable {
         boolean notifyRehashStarted = false;
         if (cmd instanceof RehashControlCommand) {
            RehashControlCommand rcc = (RehashControlCommand) cmd;
            switch (rcc.getType()) {
               case JOIN_REQ:
                  txsReady.await();
                  break;
               case PULL_STATE_JOIN:
                  notifyRehashStarted = true;
                  break;
               case JOIN_REHASH_END:
                  joinEnded.countDown();
                  break;
            }
         }

         Response r = delegate.handle(cmd);
         if (notifyRehashStarted) rehashStarted.countDown();
         return r;
      }
   }
}
