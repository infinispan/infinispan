package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.TransactionalInvocationContextFactory;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.transaction.tm.DummyBaseTransactionManager;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyXid;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(testName = "tx.TransactionXaAdapterTmIntegrationTest", groups = "unstable", description = "Disabled due to instability - see ISPN-1123 -- original group: unit")
public class TransactionXaAdapterTmIntegrationTest {
   private LocalXaTransaction localTx;
   private TransactionXaAdapter xaAdapter;
   private DummyXid xid;
   private UUID uuid = UUID.randomUUID();
   private TransactionCoordinator txCoordinator;

   @BeforeMethod
   public void setUp() throws XAException {
      Cache mockCache = mock(Cache.class);
      Configuration configuration = new ConfigurationBuilder().build();
      XaTransactionTable txTable = new XaTransactionTable();
      txTable.initialize(null, configuration, null, null, null, null,
                         null, null, null, null, mockCache, null, null, null);
      txTable.start();
      txTable.startXidMapping();
      TransactionFactory gtf = new TransactionFactory();
      gtf.init(false, false, true, false);
      GlobalTransaction globalTransaction = gtf.newGlobalTransaction(null, false);
      DummyBaseTransactionManager tm = new DummyBaseTransactionManager();
      localTx = new LocalXaTransaction(new DummyTransaction(tm), globalTransaction, false, 1, AnyEquivalence.getInstance(), 0);
      xid = new DummyXid(uuid);

      InvocationContextFactory icf = new TransactionalInvocationContextFactory();
      CommandsFactory commandsFactory = mock(CommandsFactory.class);
      InterceptorChain invoker = mock(InterceptorChain.class);
      txCoordinator = new TransactionCoordinator();
      txCoordinator.init(commandsFactory, icf, invoker, txTable, null, configuration);
      xaAdapter = new TransactionXaAdapter(localTx, txTable, null, txCoordinator, null, null,
                                           new ClusteringDependentLogic.InvalidationLogic(), configuration, "", null);

      xaAdapter.start(xid, 0);
   }

   public void testPrepareOnNonexistentXid() {
      DummyXid xid = new DummyXid(uuid);
      try {
         xaAdapter.prepare(xid);
         assert false;
      } catch (XAException e) {
         assertEquals(XAException.XAER_NOTA, e.errorCode);
      }
   }

   public void testCommitOnNonexistentXid() {
      DummyXid xid = new DummyXid(uuid);
      try {
         xaAdapter.commit(xid, false);
         assert false;
      } catch (XAException e) {
         assertEquals(XAException.XAER_NOTA, e.errorCode);
      }
   }

   public void testRollabckOnNonexistentXid() {
      DummyXid xid = new DummyXid(uuid);
      try {
         xaAdapter.rollback(xid);
         assert false;
      } catch (XAException e) {
         assertEquals(XAException.XAER_NOTA, e.errorCode);
      }
   }

   public void testPrepareTxMarkedForRollback() {
      localTx.markForRollback(true);
      try {
         xaAdapter.prepare(xid);
         assert false;
      } catch (XAException e) {
         assertEquals(XAException.XA_RBROLLBACK, e.errorCode);
      }
   }

   public void testOnePhaseCommitConfigured() throws XAException {
      Configuration configuration = new ConfigurationBuilder().clustering().cacheMode(CacheMode.INVALIDATION_ASYNC).build();
      txCoordinator.init(null, null, null, null, null, configuration);
      assert XAResource.XA_OK == xaAdapter.prepare(xid);
   }

   public void test1PcAndNonExistentXid() {
      Configuration configuration = new ConfigurationBuilder().clustering().cacheMode(CacheMode.INVALIDATION_ASYNC).build();
      txCoordinator.init(null, null, null, null, null, configuration);
      try {
         DummyXid doesNotExists = new DummyXid(uuid);
         xaAdapter.commit(doesNotExists, false);
         assert false;
      } catch (XAException e) {
         assertEquals(XAException.XAER_NOTA, e.errorCode);
      }
   }

   public void test1PcAndNonExistentXid2() {
      Configuration configuration = new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_SYNC).build();
      txCoordinator.init(null, null, null, null, null, configuration);
      try {
         DummyXid doesNotExists = new DummyXid(uuid);
         xaAdapter.commit(doesNotExists, true);
         assert false;
      } catch (XAException e) {
         assertEquals(XAException.XAER_NOTA, e.errorCode);
      }
   }
}
