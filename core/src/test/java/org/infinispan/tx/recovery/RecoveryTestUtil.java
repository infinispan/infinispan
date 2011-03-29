package org.infinispan.tx.recovery;

import org.infinispan.Cache;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import static org.testng.Assert.assertEquals;

public class RecoveryTestUtil {

   public static int count = 0;

   public static void commitTransaction(DummyTransaction dtx) throws XAException {
      TransactionXaAdapter xaResource = (TransactionXaAdapter) dtx.firstEnlistedResource();
      xaResource.commit(xaResource.getLocalTransaction().getXid(), false);
   }

   public static void prepareTransaction(DummyTransaction suspend1) throws XAException {
      TransactionXaAdapter xaResource = (TransactionXaAdapter) suspend1.firstEnlistedResource();
      xaResource.prepare(xaResource.getLocalTransaction().getXid());
   }

   public static void assertPrepared(int count, DummyTransaction...tx) throws XAException {
      for (DummyTransaction dt : tx) {
         TransactionXaAdapter xaRes = (TransactionXaAdapter) dt.firstEnlistedResource();
         assertEquals(count, xaRes.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN).length);
      }
   }

   public static RecoveryManagerImpl rm(Cache cache) {
      return (RecoveryManagerImpl) TestingUtil.extractComponentRegistry(cache).getComponent(RecoveryManager.class);
   }

   public static DummyTransaction beginAndSuspendTx(Cache cache) throws NotSupportedException, SystemException {
      DummyTransactionManager dummyTm = (DummyTransactionManager) TestingUtil.getTransactionManager(cache);
      dummyTm.begin();
      String key = "k" + count++;
      System.out.println("key = " + key);
      cache.put(key, "v");
      return (DummyTransaction) dummyTm.suspend();
   }
}
