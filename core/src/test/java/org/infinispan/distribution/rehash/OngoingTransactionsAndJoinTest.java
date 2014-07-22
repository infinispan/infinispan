package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.replaceField;

/**
 * This tests the following scenario:
 * <p/>
 * 1 node exists.  Transactions running.  Some complete, some in prepare, some in commit. New node joins, rehash occurs.
 * Test that the new node is the owner and receives this state.
 */
@Test(groups = "unstable", testName = "distribution.rehash.OngoingTransactionsAndJoinTest",
      description = "See ISPN-4044 -- original group: functional")
@CleanupAfterMethod
public class OngoingTransactionsAndJoinTest extends MultipleCacheManagersTest {
   ConfigurationBuilder configuration;
   ScheduledExecutorService delayedExecutor = Executors.newScheduledThreadPool(1);

   @Override
   protected void createCacheManagers() throws Throwable {
      configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configuration.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      configuration.locking().lockAcquisitionTimeout(60000).useLockStriping(false);
      configuration.clustering().stateTransfer().timeout(30, SECONDS);
      addClusterEnabledCacheManager(configuration);
   }

   public void testRehashOnJoin() throws InterruptedException {
      Cache<Object, Object> firstNode = cache(0);
      final CountDownLatch txsStarted = new CountDownLatch(3), txsReady = new CountDownLatch(3), joinEnded = new CountDownLatch(1), rehashStarted = new CountDownLatch(1);
      ListeningHandler listeningHandler = new ListeningHandler(extractComponent(firstNode, PerCacheInboundInvocationHandler.class), txsReady, joinEnded, rehashStarted);
      replaceComponent(firstNode, PerCacheInboundInvocationHandler.class, listeningHandler, true);
      replaceField(listeningHandler, "inboundInvocationHandler", firstNode.getAdvancedCache().getComponentRegistry(), ComponentRegistry.class);

      assert firstNode.getAdvancedCache().getComponentRegistry().getComponent(PerCacheInboundInvocationHandler.class) instanceof ListeningHandler;

      for (int i = 0; i < 10; i++) firstNode.put("OLD" + i, "value");

      UnpreparedDuringRehashTask ut = new UnpreparedDuringRehashTask(firstNode, txsStarted, txsReady, joinEnded, rehashStarted);
      PrepareDuringRehashTask pt = new PrepareDuringRehashTask(firstNode, txsStarted, txsReady, joinEnded, rehashStarted);
      CommitDuringRehashTask ct = new CommitDuringRehashTask(firstNode, txsStarted, txsReady, joinEnded, rehashStarted);

      InterceptorChain ic = TestingUtil.extractComponent(firstNode, InterceptorChain.class);
      ic.addInterceptorAfter(pt, TxInterceptor.class);
      ic.addInterceptorAfter(ct, TxInterceptor.class);


      Set<Thread> threads = new HashSet<>();
      threads.add(new Thread(ut, "Worker-UnpreparedDuringRehashTask"));
      threads.add(new Thread(pt, "Worker-PrepareDuringRehashTask"));
      threads.add(new Thread(ct, "Worker-CommitDuringRehashTask"));

      for (Thread t : threads) t.start();

      txsStarted.await(10, SECONDS);

      // we don't have a hook for the start of the rehash any more
      delayedExecutor.schedule(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            rehashStarted.countDown();
            return null;
         }
      }, 10, TimeUnit.MILLISECONDS);

      // start a new node!
      addClusterEnabledCacheManager(configuration);

      ListeningHandler listeningHandler2 = new ListeningHandler(extractComponent(firstNode, PerCacheInboundInvocationHandler.class), txsReady, joinEnded, rehashStarted);
      replaceComponent(cache(1), PerCacheInboundInvocationHandler.class, listeningHandler2, true);

      Cache<?, ?> joiner = cache(1);

      for (Thread t : threads) t.join();

      TestingUtil.waitForRehashToComplete(cache(0), cache(1));

      for (int i = 0; i < 10; i++) {
         Object key = "OLD" + i;
         Object value = joiner.get(key);
         log.infof(" TEST: Key %s is %s", key, value);
         assert "value".equals(value) : "Couldn't see key " + key + " on joiner!";
      }

      for (Object key: Arrays.asList(ut.key(), pt.key(), ct.key())) {
         Object value = joiner.get(key);
         log.infof(" TEST: Key %s is %s", key, value);
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
         tx.enlistResource(new XAResourceAdapter()); // this is to force 2PC and to prevent transaction managers attempting to optimise the call to a 1PC.
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

      @Override
      Object key() {
         return "unprepared_during_rehash";
      }

      @Override
      public void run() {
         try {
            // start a tx
            startTx();
            txsReady.countDown();
            joinEnded.await(10, SECONDS);
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

      @Override
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
         if (tx.equals(tcx.getTransaction())) {
            txsReady.countDown();
            rehashStarted.await(10, SECONDS);
         }
         return super.visitPrepareCommand(tcx, cc);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext tcx, CommitCommand cc) throws Throwable {
         if (tx.equals(tcx.getTransaction())) {
            joinEnded.await(10, SECONDS);
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

      @Override
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
         if (tx.equals(tcx.getTransaction())) {
            txsReady.countDown();
         }
         return o;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext tcx, CommitCommand cc) throws Throwable {
         if (tx.equals(tcx.getTransaction())) {
            rehashStarted.await(10, SECONDS);
         }

         return super.visitCommitCommand(tcx, cc);
      }
   }

   class ListeningHandler implements PerCacheInboundInvocationHandler {
      final PerCacheInboundInvocationHandler delegate;
      final CountDownLatch txsReady, joinEnded, rehashStarted;

      public ListeningHandler(PerCacheInboundInvocationHandler delegate, CountDownLatch txsReady, CountDownLatch joinEnded, CountDownLatch rehashStarted) {
         this.delegate = delegate;
         this.txsReady = txsReady;
         this.joinEnded = joinEnded;
         this.rehashStarted = rehashStarted;
      }

      @Override
      public void handle(CacheRpcCommand cmd, Reply reply, DeliverOrder order) {
         boolean notifyRehashStarted = false;
         if (cmd instanceof CacheTopologyControlCommand) {
            CacheTopologyControlCommand rcc = (CacheTopologyControlCommand) cmd;
            log.debugf("Intercepted command: %s", cmd);
            switch (rcc.getType()) {
               case REBALANCE_START:
                  try {
                     txsReady.await(10, SECONDS);
                  } catch (InterruptedException e) {
                     Thread.currentThread().interrupt();
                     reply.reply(new ExceptionResponse(e));
                     return;
                  }
                  notifyRehashStarted = true;
                  break;
               case CH_UPDATE:
                  // TODO Use another type instead, e.g. REBASE_END
                  joinEnded.countDown();
                  break;
            }
         }

         delegate.handle(cmd, reply, order);
         if (notifyRehashStarted) rehashStarted.countDown();
      }
   }

}
