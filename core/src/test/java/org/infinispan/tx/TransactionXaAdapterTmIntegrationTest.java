package org.infinispan.tx;

import org.infinispan.config.Configuration;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyXid;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(testName = "tx.TransactionXaAdapterTmIntegrationTest", groups = "unit")
public class TransactionXaAdapterTmIntegrationTest {
   private Configuration configuration;
   private XaTransactionTable txTable;
   private GlobalTransaction globalTransaction;
   private LocalXaTransaction localTx;
   private TransactionXaAdapter xaAdapter;
   private DummyXid xid;

   @BeforeMethod
   public void setUp() {
      txTable = new XaTransactionTable();
      TransactionFactory gtf = new TransactionFactory();
      globalTransaction = gtf.newGlobalTransaction(null, false);
      localTx = new LocalXaTransaction(new DummyTransaction(null), globalTransaction);
      xid = new DummyXid();
      localTx.setXid(xid);
      txTable.addLocalTransactionMapping(localTx);      

      configuration = new Configuration();
      TransactionCoordinator txCoordinator = new TransactionCoordinator();
      txCoordinator.init(null, null, null, null, configuration);
      xaAdapter = new TransactionXaAdapter(localTx, txTable, configuration, null, null, txCoordinator);
   }

   public void testPrepareOnNonexistentXid() {
      DummyXid xid = new DummyXid();
      try {
         xaAdapter.prepare(xid);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }

   public void testCommitOnNonexistentXid() {
      DummyXid xid = new DummyXid();
      try {
         xaAdapter.commit(xid, false);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }

   public void testRollabckOnNonexistentXid() {
      DummyXid xid = new DummyXid();
      try {
         xaAdapter.rollback(xid);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }

   public void testPrepareTxMarkedForRollback() {
      localTx.markForRollback();
      try {
         xaAdapter.prepare(xid);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XA_RBROLLBACK;
      }
   }

   public void testOnePhaseCommitConfigured() throws XAException {
      configuration.setCacheMode(Configuration.CacheMode.INVALIDATION_ASYNC);//this would force 1pc
      assert XAResource.XA_OK == xaAdapter.prepare(xid);
   }

   public void test1PcAndNonExistentXid() {
      configuration.setCacheMode(Configuration.CacheMode.INVALIDATION_ASYNC);
      try {
         DummyXid doesNotExists = new DummyXid();
         xaAdapter.commit(doesNotExists, false);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }

   public void test1PcAndNonExistentXid2() {
      configuration.setCacheMode(Configuration.CacheMode.DIST_SYNC);
      try {
         DummyXid doesNotExists = new DummyXid();
         xaAdapter.commit(doesNotExists, true);
         assert false;
      } catch (XAException e) {
         assert e.errorCode == XAException.XAER_NOTA;
      }
   }
}
