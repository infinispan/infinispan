package org.infinispan.distribution;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;

@Test(groups = "functional", enabled=false, testName = "distribution.DistSyncTxFuncTest")
public class DistSyncTxFuncTest extends BaseDistFunctionalTest<Object, String> {
   
   public DistSyncTxFuncTest() {
      sync = true;
      tx = true;
      testRetVals = true;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
   }

   protected void asyncTxWait(Object... keys) {
      // no op.  Meant to be overridden
   }

   protected void init(MagicKey k1, MagicKey k2) {
      // neither key maps on to c4
      c2.put(k1, "value1");
      asyncWait(k1, PutKeyValueCommand.class, c1, c3, c4);

      c2.put(k2, "value2");
      asyncWait(k2, PutKeyValueCommand.class, c1, c3, c4);

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
//    we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2); // maps on to c2 and c3

      init(k1, k2);

      // now test a transaction that spans both keys.
      TransactionManager tm4 = getTransactionManager(c4);
      asserLocked(c3, false, k1);
      tm4.begin();
      c4.put(k1, "new_value1");
      c4.put(k2, "new_value2");
      tm4.commit();

      asyncTxWait("new_value1", "new_value2");

      asserLocked(c3, false, k1);
      asserLocked(c3, false, k2);

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      asserLocked(c4, false, k1, k2);
      asserLocked(c3, false, k1);
      asserLocked(c3, false, k2);
      asserLocked(c1, false, k1, k2);
      asserLocked(c2, false, k1, k2);
      checkOwnership(k1, k2, "new_value1", "new_value2");
   }

   void asserLocked(Cache c, boolean isLocked, Object... keys) {
      LockManager lm = TestingUtil.extractComponent(c, LockManager.class);
      for (Object key : keys) {
         assert isLocked == lm.isLocked(key) : " expecting key '" + key + "' to be " + (isLocked ? " locked " :
               "not locked + \n Lock owner is:" + lm.getOwner(key));
      }
   }

   protected void checkOwnership(MagicKey k1, MagicKey k2, String v1, String v2) {
      assertOnAllCachesAndOwnership(k1, v1);
      assertOnAllCachesAndOwnership(k2, v2);

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsInL1(c1, k2);
      assertIsInL1(c3, k1);
   }

   public void testTransactionsSpanningKeysRollback() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2); // maps on to c2 and c3

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
      MagicKey k1 = new MagicKey("k1", c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      Object ret = c4.put(k1, "new_value");
      if (testRetVals) assert "value1".equals(ret);
      ret = c4.put(k2, "new_value");
      if (testRetVals) assert "value2".equals(ret);
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
      MagicKey k1 = new MagicKey("k1", c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      LockManager lockManager4 = TestingUtil.extractComponent(c4, LockManager.class);

      tm4.begin();
      Object ret = c4.putIfAbsent(k1, "new_value");
      if (testRetVals) assert "value1".equals(ret) : "Was expecting value1 but was " + ret;
      ret = c4.putIfAbsent(k2, "new_value");
      if (testRetVals) assert "value2".equals(ret) : "Was expecting value2 but was " + ret;

      assert c4.get(k1).equals("value1");
      assert c4.get(k2).equals("value2");

      assert lockManager4.isLocked(k1);
      assert lockManager4.isLocked(k2);

      tm4.rollback();

      assert !lockManager4.isLocked(k1);
      assert !lockManager4.isLocked(k2);

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
      MagicKey k1 = new MagicKey("k1", c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2); // maps on to c2 and c3

      init(k1, k2);
      asserLocked(c1, false, k1, k2);
      asserLocked(c2, false, k1, k2);
      asserLocked(c3, false, k1, k2);
      asserLocked(c4, false, k1, k2);
      

      log.info("***** Here it starts!");
      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      Object ret = c4.remove(k1);
      if (testRetVals) assert "value1".equals(ret);
      ret = c4.remove(k2);
      if (testRetVals) assert "value2".equals(ret);

      assert !c4.containsKey(k1);
      assert !c4.containsKey(k2);
      tm4.rollback();
      log.info("----- Here it ends!");

      asserLocked(c1, false, k1, k2);
      asserLocked(c2, false, k1, k2);
      asserLocked(c3, false, k1, k2);
      asserLocked(c4, false, k1 );
      asserLocked(c4, false, k2 );

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      asserLocked(c1, false, k1, k2);
      asserLocked(c2, false, k1, k2);
      asserLocked(c3, false, k1, k2);
      asserLocked(c4, false, k1, k2);


      assertIsNotInL1(c4, k1);
      assertIsNotInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      asserLocked(c1, false, k1, k2);
      asserLocked(c2, false, k1, k2);
      asserLocked(c3, false, k1, k2);
      asserLocked(c4, false, k1, k2);
      
      checkOwnership(k1, k2, "value1", "value2");
   }


   public void testConditionalRemoveFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      boolean ret = c4.remove(k1, "valueX");
      if (testRetVals) assert !ret;
      ret = c4.remove(k2, "valueX");
      if (testRetVals) assert !ret;

      assert c4.containsKey(k1);
      assert c4.containsKey(k2);

      ret = c4.remove(k1, "value1");
      if (testRetVals) assert ret;
      ret = c4.remove(k2, "value2");
      if (testRetVals) assert ret;

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
      MagicKey k1 = new MagicKey("k1", c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      Object ret = c4.replace(k1, "new_value");
      if (testRetVals) assert "value1".equals(ret);
      ret = c4.replace(k2, "new_value");
      if (testRetVals) assert "value2".equals(ret);

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
      MagicKey k1 = new MagicKey("k1", c1); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      boolean ret = c4.replace(k1, "valueX", "new_value");
      if (testRetVals) assert !ret;
      ret = c4.replace(k2, "valueX", "new_value");
      if (testRetVals) assert !ret;

      assert "value1".equals(c4.get(k1));
      assert "value2".equals(c4.get(k2));

      ret = c4.replace(k1, "value1", "new_value");
      if (testRetVals) assert ret;
      ret = c4.replace(k2, "value2", "new_value");
      if (testRetVals) assert ret;

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
