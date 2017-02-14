package org.infinispan.tx.recovery;

import static org.testng.Assert.assertEquals;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;

public class RecoveryTestUtil {

   public static int count = 0;

   public static void commitTransaction(EmbeddedTransaction dtx) throws XAException {
      TransactionXaAdapter xaResource = (TransactionXaAdapter) dtx.firstEnlistedResource();
      xaResource.commit(xaResource.getLocalTransaction().getXid(), false);
   }

   public static void rollbackTransaction(EmbeddedTransaction dtx) throws XAException {
      TransactionXaAdapter xaResource = (TransactionXaAdapter) dtx.firstEnlistedResource();
      xaResource.commit(xaResource.getLocalTransaction().getXid(), false);
   }

   public static void prepareTransaction(EmbeddedTransaction suspend1) {
      TransactionXaAdapter xaResource = (TransactionXaAdapter) suspend1.firstEnlistedResource();
      try {
         xaResource.prepare(xaResource.getLocalTransaction().getXid());
      } catch (XAException e) {
         throw new RuntimeException(e);
      }
   }

   static void assertPrepared(int count, EmbeddedTransaction... tx) throws XAException {
      for (EmbeddedTransaction dt : tx) {
         TransactionXaAdapter xaRes = (TransactionXaAdapter) dt.firstEnlistedResource();
         assertEquals(count, xaRes.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
      }
   }

   public static RecoveryManagerImpl rm(Cache cache) {
      return (RecoveryManagerImpl) TestingUtil.extractComponentRegistry(cache).getComponent(RecoveryManager.class);
   }

   public static EmbeddedTransaction beginAndSuspendTx(Cache cache) {
      return beginAndSuspendTx(cache, "k" + count++);
   }

   public static EmbeddedTransaction beginAndSuspendTx(Cache cache, Object key) {
      EmbeddedTransactionManager dummyTm = (EmbeddedTransactionManager) TestingUtil.getTransactionManager(cache);
      try {
         dummyTm.begin();
         cache.put(key, "v");
         return (EmbeddedTransaction) dummyTm.suspend();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }
}
