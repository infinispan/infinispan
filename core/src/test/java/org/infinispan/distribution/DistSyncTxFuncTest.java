package org.infinispan.distribution;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "distribution.DistSyncTxFuncTest")
public class DistSyncTxFuncTest extends BaseDistFunctionalTest {
   public DistSyncTxFuncTest() {
      sync = true;
      tx = true;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
   }

   protected void asyncTxWait(Object... keys) {
      // no op.  Meant to be overridden
   }

   private void init(MagicKey k1, MagicKey k2) {
      // neither key maps on to c4
      c2.put(k1, "value1");
      asyncWait(k1, PutKeyValueCommand.class);

      c2.put(k2, "value2");
      asyncWait(k2, PutKeyValueCommand.class);

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

      asyncTxWait(k1, k2);

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "new_value1", "new_value2");
   }

   private void checkOwnership(MagicKey k1, MagicKey k2, String v1, String v2) {
      assertOnAllCachesAndOwnership(k1, v1);
      assertOnAllCachesAndOwnership(k2, v2);

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

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testPutFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey(c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey(c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      Object ret = c4.put(k1, "new_value");
      assert "value1".equals(ret);
      ret = c4.put(k2, "new_value");
      assert "value2".equals(ret);
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testPutIfAbsentFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey(c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey(c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      Object ret = c4.putIfAbsent(k1, "new_value");
      assert "value1".equals(ret) : "Was expecting value1 but was " + ret;
      ret = c4.putIfAbsent(k2, "new_value");
      assert "value2".equals(ret) : "Was expecting value2 but was " + ret;

      assert c4.get(k1).equals("value1");
      assert c4.get(k2).equals("value2");

      tm4.rollback();

      assert c2.get(k1).equals("value1");
      assert c2.get(k2).equals("value2");

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testRemoveFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey(c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey(c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      Object ret = c4.remove(k1);
      assert "value1".equals(ret);
      ret = c4.remove(k2);
      assert "value2".equals(ret);

      assert !c4.containsKey(k1);
      assert !c4.containsKey(k2);
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testConditionalRemoveFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey(c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey(c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      boolean ret = c4.remove(k1, "valueX");
      assert !ret;
      ret = c4.remove(k2, "valueX");
      assert !ret;

      assert c4.containsKey(k1);
      assert c4.containsKey(k2);

      ret = c4.remove(k1, "value1");
      assert ret;
      ret = c4.remove(k2, "value2");
      assert ret;

      assert !c4.containsKey(k1);
      assert !c4.containsKey(k2);
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testReplaceFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey(c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey(c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      Object ret = c4.replace(k1, "new_value");
      assert "value1".equals(ret);
      ret = c4.replace(k2, "new_value");
      assert "value2".equals(ret);

      assert "new_value".equals(c4.get(k1));
      assert "new_value".equals(c4.get(k2));
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testConditionalReplaceFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey(c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey(c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      boolean ret = c4.replace(k1, "valueX", "new_value");
      assert !ret;
      ret = c4.replace(k2, "valueX", "new_value");
      assert !ret;

      assert "value1".equals(c4.get(k1));
      assert "value2".equals(c4.get(k2));

      ret = c4.replace(k1, "value1", "new_value");
      assert ret;
      ret = c4.replace(k2, "value2", "new_value");
      assert ret;

      assert "new_value".equals(c4.get(k1));
      assert "new_value".equals(c4.get(k2));
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }
}
