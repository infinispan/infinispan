package org.infinispan.distribution;

import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "distribution.DistSyncTxFuncTest", enabled = false)
public class DistSyncTxFuncTest extends BaseDistFunctionalTest {
   public DistSyncTxFuncTest() {
      sync = true;
      tx = true;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
   }

   private void init(MagicKey k1, MagicKey k2) {
      // neither key maps on to c4
      c2.put(k1, "value1");
      c2.put(k2, "value2");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);
   }

   public void testTransactionsSpanningKeysCommit() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey(c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey(c2); // maps on to c2 and c3

      init(k1, k2);

      // now test a transaction that spans both keys.
      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      c4.put(k1, "new_value1");
      c4.put(k2, "new_value2");
      tm4.commit();

      asyncWait();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      assertOnAllCachesAndOwnership(k1, "new_value1");
      assertOnAllCachesAndOwnership(k2, "new_value2");

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsInL1(c1, k2);
      assertIsInL1(c3, k1);
   }

   public void testTransactionsSpanningKeysRollback() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey(c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey(c2); // maps on to c2 and c3

      init(k1, k2);

      // now test a transaction that spans both keys.
      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      c4.put(k1, "new_value1");
      c4.put(k2, "new_value2");
      tm4.rollback();

      asyncWait();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      assertOnAllCachesAndOwnership(k1, "value1");
      assertOnAllCachesAndOwnership(k2, "value2");

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsInL1(c1, k2);
      assertIsInL1(c3, k1);
   }

   public void testReturnValuesInTx() throws Exception {
      // TODO
   }
}
