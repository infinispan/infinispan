package org.infinispan.server.hotrod.tx;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.wrapComponent;
import static org.infinispan.util.ByteString.fromString;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.counter.response.RecoveryTestResponse;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.ClientAddress;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.server.hotrod.tx.table.PerCacheTxTable;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.server.hotrod.tx.table.functions.CreateStateFunction;
import org.infinispan.server.hotrod.tx.table.functions.PreparingDecisionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetCompletedTransactionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetDecisionFunction;
import org.infinispan.server.hotrod.tx.table.functions.SetPreparedFunction;
import org.infinispan.server.hotrod.tx.table.functions.TxFunction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.AbstractDelegatingRpcManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Synchronization;

/**
 * Transaction Recovery and Reaper test.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@Test(groups = "functional", testName = "server.hotrod.tx.TxReaperAndRecoveryTest")
public class TxReaperAndRecoveryTest extends HotRodMultiNodeTest {

   private static final AtomicInteger XID_GENERATOR = new AtomicInteger(1);
   private final ControlledTimeService timeService = new ControlledTimeService();

   private static XidImpl newXid() {
      byte id = (byte) XID_GENERATOR.getAndIncrement();
      return XidImpl.create(-123456, new byte[]{id}, new byte[]{id});
   }

   private static Address newAddress() {
      //test address isn't serializable and we just need an address that doesn't belong to the cluster (simulates a leaver)
      return new JGroupsAddress(org.jgroups.util.UUID.randomUUID());
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      for (EmbeddedCacheManager cm : cacheManagers) {
         //use the same time service in all managers
         replaceComponent(cm, TimeService.class, timeService, true);
         //stop reaper. we are going to trigger it manually
         extractGlobalComponent(cm, GlobalTxTable.class).stop();
      }
   }

   public void testCleanup() throws InterruptedException {
      XidImpl xid1 = newXid();
      XidImpl xid2 = newXid();
      XidImpl xid3 = newXid();
      XidImpl xid4 = newXid();

      //Xid1 will be committed, Xid2 will be rolled-back
      initGlobalTxTable(0, xid1, null, false, Status.COMMITTED);
      initGlobalTxTable(1, xid2, null, false, Status.ROLLED_BACK);
      initGlobalTxTable(1, xid3, newAddress(), false, Status.COMMITTED);
      initGlobalTxTable(1, xid4, newAddress(), false, Status.ROLLED_BACK);


      //check pre state
      assertStatus(false, false, xid1, xid2, xid3, xid4);

      timeService.advance(1);

      //check that state is not timeout
      assertStatus(false, false, xid1, xid2, xid3, xid4);

      globalTxTable(0).run();
      //remove is async and it shouldn't happen
      //if it happens, we will get a NPE later in the test
      Thread.sleep(1000);

      timeService.advance(1);

      assertStatus(true, false, xid1, xid2, xid3, xid4);

      //it only cleanup locally originated
      //and transaction where the originator doesn't belong to the view (i.e. xid1 and xid3)
      globalTxTable(0).run();

      eventually(() -> checkNotExists(xid1, xid3, xid4));

      assertStatus(true, false, xid2);

      globalTxTable(1).run();

      eventually(() -> checkNotExists(xid2));

      assertTrue(globalTxTable(0).isEmpty());
   }


   public void testRollbackIdleTransactions() throws RollbackException {
      XidImpl xid1 = newXid();
      XidImpl xid2 = newXid();
      XidImpl xid3 = newXid();

      //xid1 active status
      initGlobalTxTable(0, xid1, null, false, Status.ACTIVE);

      //xid2 preparing status
      initGlobalTxTable(1, xid2, null, false, Status.PREPARING);

      //xid3 prepared
      initGlobalTxTable(1, xid3, newAddress(), false, Status.PREPARED);

      //check pre state
      assertStatus(false, false, xid1, xid2, xid3);

      timeService.advance(2);

      assertStatus(true, false, xid1, xid2, xid3);


      EmbeddedTransaction tx1 = newTx(xid1);
      LoggingSynchronization sync = new LoggingSynchronization();
      tx1.registerSynchronization(sync);
      perCacheTxTable(0).createLocalTx(xid1, tx1);
      LoggingRpcManager rpcManager0 = rpcManager();
      rpcManager0.queue.clear();

      globalTxTable(0).run();
      //it should fetch the embedded tx and rollback it
      eventually(() -> "rolled_back".equals(sync.queue.poll()));
      eventually(() -> "rollback".equals(rpcManager0.queue.poll()));
      eventually(() -> getState(xid1) == null);
      //it doesn't remove it right away.
      eventually(() -> getState(xid3).getStatus() == Status.ROLLED_BACK);

      EmbeddedTransaction tx2 = newTx(xid2);
      sync.queue.clear();
      tx2.registerSynchronization(sync);
      perCacheTxTable(1).createLocalTx(xid2, tx2);

      globalTxTable(1).run();
      eventually(() -> "rolled_back".equals(sync.queue.poll()));
      eventually(() -> getState(xid2) == null);

      assertStatus(false, false, xid3);

      timeService.advance(2);

      globalTxTable(0).run();

      eventually(() -> globalTxTable(0).isEmpty());
   }

   public void testPartialCompletedTransactions() throws RollbackException {
      XidImpl xid1 = newXid();
      XidImpl xid2 = newXid();
      XidImpl xid3 = newXid();
      XidImpl xid4 = newXid();

      //xid1 mark_to_commit and local
      initGlobalTxTable(0, xid1, null, false, Status.MARK_COMMIT);

      //xid2 mark_to_rollback and remote
      initGlobalTxTable(1, xid2, null, false, Status.MARK_ROLLBACK);

      //xid3 mark_to_commit and leaver
      initGlobalTxTable(1, xid3, newAddress(), false, Status.MARK_COMMIT);

      //xid4 mark_to_commit and leaver
      initGlobalTxTable(1, xid4, newAddress(), false, Status.MARK_ROLLBACK);

      //check pre state
      assertStatus(false, false, xid1, xid2, xid3, xid4);

      timeService.advance(2);

      assertStatus(true, false, xid1, xid2, xid3, xid4);

      EmbeddedTransaction tx1 = newTx(xid1);
      LoggingSynchronization sync = new LoggingSynchronization();
      tx1.registerSynchronization(sync);
      perCacheTxTable(0).createLocalTx(xid1, tx1);
      LoggingRpcManager rpcManager0 = rpcManager();
      rpcManager0.queue.clear();

      globalTxTable(0).run();

      //it should fetch the embedded tx and rollback it
      eventually(() -> "committed".equals(sync.queue.poll()));
      //we don't know the order of the operations
      eventuallyEquals(2, rpcManager0.queue::size);
      Set<String> actual = new HashSet<>(rpcManager0.queue);
      Set<String> expected = new HashSet<>(Arrays.asList("rollback", "prepare"));
      assertEquals(actual, expected);
      eventually(() -> getState(xid1) == null);
      //it doesn't remove it right away.
      eventually(() -> getState(xid3).getStatus() == Status.COMMITTED);
      eventually(() -> getState(xid4).getStatus() == Status.ROLLED_BACK);

      EmbeddedTransaction tx2 = newTx(xid2);
      sync.queue.clear();
      tx2.registerSynchronization(sync);
      perCacheTxTable(1).createLocalTx(xid2, tx2);

      globalTxTable(1).run();
      eventually(() -> "rolled_back".equals(sync.queue.poll()));
      eventually(() -> getState(xid2) == null);


      assertStatus(false, false, xid3, xid4);

      timeService.advance(2);

      globalTxTable(0).run();

      eventually(() -> globalTxTable(0).isEmpty());
   }

   public void testRecovery() {
      XidImpl xid1 = newXid();
      XidImpl xid2 = newXid();
      XidImpl xid3 = newXid();
      XidImpl xid4 = newXid();

      //xid1 local and prepared
      initGlobalTxTable(0, xid1, null, true, Status.PREPARED);
      //xid2 remote and prepared
      initGlobalTxTable(1, xid2, null, true, Status.PREPARED);
      //xid3 non-member and prepared
      initGlobalTxTable(1, xid3, newAddress(), true, Status.PREPARED);
      //xid4 non-member and preparing
      initGlobalTxTable(1, xid4, newAddress(), false, Status.PREPARING);

      assertStatus(false, true, xid1, xid2, xid3);
      assertStatus(false, false, xid4);
      timeService.advance(2);
      assertStatus(true, true, xid1, xid2, xid3);
      assertStatus(true, false, xid4);

      //only xid4 should have make progress
      globalTxTable(0).run();
      eventually(() -> getState(xid4).getStatus() == Status.ROLLED_BACK);
      assertEquals(Status.PREPARED, getState(xid1).getStatus());
      assertEquals(Status.PREPARED, getState(xid2).getStatus());
      assertEquals(Status.PREPARED, getState(xid3).getStatus());

      Set<XidImpl> actual = new HashSet<>(globalTxTable(0).getPreparedTransactions());
      Set<XidImpl> expected = new HashSet<>(Arrays.asList(xid1, xid2, xid3));
      assertEquals(expected, actual);
      assertStatus(true, true, xid1, xid2, xid3);

      timeService.advance(2);
      globalTxTable(0).run();
      eventually(() -> getState(xid4) == null);
      assertEquals(Status.PREPARED, getState(xid1).getStatus());
      assertEquals(Status.PREPARED, getState(xid2).getStatus());
      assertEquals(Status.PREPARED, getState(xid3).getStatus());

      TestResponse response = clients().get(0).recovery();
      assertTrue(response instanceof RecoveryTestResponse);
      actual = new HashSet<>(((RecoveryTestResponse) response).getXids());
      assertEquals(expected, actual);

      for (XidImpl xid : expected) {
         clients().get(0).rollbackTx(xid);
         clients().get(0).forgetTx(xid);
      }
      assertTrue(globalTxTable(0).isEmpty());
   }

   @Override
   protected String cacheName() {
      return "tx-reaper-and-recovery";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      //TODO optimistic (when it is supported)
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return builder;
   }

   private TxState getState(XidImpl xid) {
      return globalTxTable(0).getState(new CacheXid(fromString(cacheName()), xid));
   }

   private void initGlobalTxTable(int index, XidImpl xid, Address address, boolean recoverable, Status status) {
      GlobalTxTable globalTxTable = globalTxTable(index);
      CacheXid cacheXid = new CacheXid(fromString(cacheName()), xid);
      List<TxFunction> functions = new ArrayList<>(5);
      GlobalTransaction gtx = address == null ?
                              newGlobalTransaction(cacheName(), index) :
                              newGlobalTransaction(cacheName(), index, address);

      switch (status) {
         case ACTIVE:
            functions.add(new CreateStateFunction(gtx, recoverable, 1));
            break;
         case PREPARING:
            functions.add(new CreateStateFunction(gtx, recoverable, 1));
            functions.add(new PreparingDecisionFunction(Collections.emptyList()));
            break;
         case PREPARED:
            functions.add(new CreateStateFunction(gtx, recoverable, 1));
            functions.add(new PreparingDecisionFunction(Collections.emptyList()));
            functions.add(new SetPreparedFunction());
            break;
         case MARK_ROLLBACK:
            functions.add(new CreateStateFunction(gtx, recoverable, 1));
            functions.add(new PreparingDecisionFunction(Collections.emptyList()));
            functions.add(new SetPreparedFunction());
            functions.add(new SetDecisionFunction(false));
            break;
         case MARK_COMMIT:
            functions.add(new CreateStateFunction(gtx, recoverable, 1));
            functions.add(new PreparingDecisionFunction(Collections.emptyList()));
            functions.add(new SetPreparedFunction());
            functions.add(new SetDecisionFunction(true));
            break;
         case ROLLED_BACK:
            functions.add(new CreateStateFunction(gtx, recoverable, 1));
            functions.add(new PreparingDecisionFunction(Collections.emptyList()));
            functions.add(new SetPreparedFunction());
            functions.add(new SetDecisionFunction(false));
            functions.add(new SetCompletedTransactionFunction(false));
            break;
         case COMMITTED:
            functions.add(new CreateStateFunction(gtx, recoverable, 1));
            functions.add(new PreparingDecisionFunction(Collections.emptyList()));
            functions.add(new SetPreparedFunction());
            functions.add(new SetDecisionFunction(true));
            functions.add(new SetCompletedTransactionFunction(true));
            break;
         default:
            throw new IllegalStateException();
      }

      for (TxFunction function : functions) {
         assertEquals(Status.OK, globalTxTable.update(cacheXid, function, 30000));
      }

      assertEquals(status, globalTxTable.getState(cacheXid).getStatus());
   }

   private PerCacheTxTable perCacheTxTable(int index) {
      return extractComponent(cache(index, cacheName()), PerCacheTxTable.class);
   }

   private EmbeddedTransaction newTx(XidImpl xid) {
      EmbeddedTransaction tx = new EmbeddedTransaction(EmbeddedTransactionManager.getInstance());
      tx.setXid(xid);
      return tx;
   }

   private LoggingRpcManager rpcManager() {
      RpcManager rpcManager = extractComponent(cache(0, cacheName()), RpcManager.class);
      if (rpcManager instanceof LoggingRpcManager) {
         return (LoggingRpcManager) rpcManager;
      }
      return wrapComponent(cache(0, cacheName()), RpcManager.class, LoggingRpcManager::new);
   }

   private boolean checkNotExists(XidImpl... xids) {
      for (XidImpl xid : xids) {
         CacheXid cacheXid = new CacheXid(fromString(cacheName()), xid);
         if (globalTxTable(0).getState(cacheXid) != null) {
            return false;
         }
      }
      return true;
   }

   private void assertStatus(boolean timeout, boolean recoverable, XidImpl... xids) {
      GlobalTxTable globalTxTable = globalTxTable(0);
      for (XidImpl xid : xids) {
         CacheXid cacheXid = new CacheXid(fromString(cacheName()), xid);
         TxState state = globalTxTable.getState(cacheXid);
         assertEquals(recoverable, state.isRecoverable());
         assertEquals(timeout, state.hasTimedOut(timeService.time()));
      }

   }

   private GlobalTxTable globalTxTable(int index) {
      return extractGlobalComponent(cacheManagers.get(index), GlobalTxTable.class);
   }

   private GlobalTransaction newGlobalTransaction(String cacheName, int index) {
      return newGlobalTransaction(cacheName, index, address(index));
   }

   private GlobalTransaction newGlobalTransaction(String cacheName, int index, Address address) {
      TransactionFactory factory = extractComponent(cache(index, cacheName), TransactionFactory.class);
      return factory.newGlobalTransaction(new ClientAddress(address), false);
   }

   private static class LoggingSynchronization implements Synchronization {

      private final Queue<String> queue = new LinkedBlockingQueue<>();

      @Override
      public void beforeCompletion() {
         queue.add("before");
      }

      @Override
      public void afterCompletion(int status) {
         if (status == jakarta.transaction.Status.STATUS_COMMITTED) {
            queue.add("committed");
         } else {
            queue.add("rolled_back");
         }
      }
   }

   private static class LoggingRpcManager extends AbstractDelegatingRpcManager {

      private final Queue<String> queue = new LinkedBlockingQueue<>();

      private LoggingRpcManager(RpcManager realOne) {
         super(realOne);
      }

      @Override
      protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command,
                                                      ResponseCollector<T> collector,
                                                      Function<ResponseCollector<T>, CompletionStage<T>> invoker,
                                                      RpcOptions rpcOptions) {
         if (command instanceof RollbackCommand) {
            queue.add("rollback");
         } else if (command instanceof PrepareCommand) {
            queue.add("prepare");
         }
         return super.performRequest(targets, command, collector, invoker, rpcOptions);
      }
   }


}
