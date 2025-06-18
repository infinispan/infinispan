package org.infinispan.distribution;

import static java.lang.String.format;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

@Test(groups = "functional", testName = "distribution.DistSyncTxFuncTest")
public class DistSyncTxFuncTest extends BaseDistFunctionalTest<Object, String> {

   public DistSyncTxFuncTest() {
      transactional = true;
      testRetVals = true;
      cleanup = CleanupPhase.AFTER_METHOD; // ensure any stale TXs are wiped
   }

   public void testTransactionsSpanningKeysCommit() throws Exception {
//    we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1, c2); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2, c3); // maps on to c2 and c3

      init(k1, k2);

      // now test a transaction that spans both keys.
      TransactionManager tm4 = getTransactionManager(c4);
      assertNotLocked(c3, k1);
      tm4.begin();
      c4.put(k1, "new_value1");
      c4.put(k2, "new_value2");
      tm4.commit();

      assertNotLocked(c3, k1);
      assertNotLocked(c3, k2);

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      assertNotLocked(c4, k1, k2);
      assertNotLocked(c3, k1);
      assertNotLocked(c3, k2);
      assertNotLocked(c1, k1, k2);
      assertNotLocked(c2, k1, k2);
      checkOwnership(k1, k2, "new_value1", "new_value2");
   }

   public void testTransactionsSpanningKeysRollback() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1, c2); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2, c3); // maps on to c2 and c3

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

      assertTransactionOriginatorDataAfterRollback(c4, k1, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testPutFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1, c2); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2, c3); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      assertRetVal("value1", c4.put(k1, "new_value"));
      assertRetVal("value2", c4.put(k2, "new_value"));
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertTransactionOriginatorDataAfterRollback(c4, k1, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testPutIfAbsentFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1, c2); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2, c3); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      LockManager lockManager4 = extractComponent(c4, LockManager.class);

      tm4.begin();
      assertRetVal("value1", c4.putIfAbsent(k1, "new_value"));
      assertRetVal("value2", c4.putIfAbsent(k2, "new_value"));

      assertEquals("value1", c4.get(k1));
      assertEquals("value2", c4.get(k2));
      tm4.rollback();

      assertFalse(lockManager4.isLocked(k1));
      assertFalse(lockManager4.isLocked(k2));

      assertEquals("value1", c2.get(k1));
      assertEquals("value2", c2.get(k2));

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertTransactionOriginatorDataAfterRollback(c4, k1, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testRemoveFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1, c2); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2, c3); // maps on to c2 and c3

      init(k1, k2);
      assertNotLocked(c1, k1, k2);
      assertNotLocked(c2, k1, k2);
      assertNotLocked(c3, k1, k2);
      assertNotLocked(c4, k1, k2);


      log.info("***** Here it starts!");
      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      assertRetVal("value1", c4.remove(k1));
      assertRetVal("value2", c4.remove(k2));

      assertFalse(c4.containsKey(k1));
      assertFalse(c4.containsKey(k2));
      tm4.rollback();
      log.info("----- Here it ends!");

      assertNotLocked(c1, k1, k2);
      assertNotLocked(c2, k1, k2);
      assertNotLocked(c3, k1, k2);
      assertNotLocked(c4, k1);
      assertNotLocked(c4, k2);

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertNotLocked(c1, k1, k2);
      assertNotLocked(c2, k1, k2);
      assertNotLocked(c3, k1, k2);
      assertNotLocked(c4, k1, k2);


      assertTransactionOriginatorDataAfterRollback(c4, k1, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      assertNotLocked(c1, k1, k2);
      assertNotLocked(c2, k1, k2);
      assertNotLocked(c3, k1, k2);
      assertNotLocked(c4, k1, k2);

      checkOwnership(k1, k2, "value1", "value2");
   }


   public void testConditionalRemoveFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1, c2); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2, c3); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      assertRetVal(false, c4.remove(k1, "valueX"));
      assertRetVal(false, c4.remove(k1, "valueX"));

      assertTrue(c4.containsKey(k1));
      assertTrue(c4.containsKey(k2));

      assertRetVal(true, c4.remove(k1, "value1"));
      assertRetVal(true, c4.remove(k2, "value2"));

      assertFalse(c4.containsKey(k1));
      assertFalse(c4.containsKey(k2));
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testReplaceFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1, c2); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2, c3); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      assertRetVal("value1", c4.replace(k1, "new_value"));
      assertRetVal("value2", c4.replace(k2, "new_value"));

      assertEquals("new_value", c4.get(k1));
      assertEquals("new_value", c4.get(k2));
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      assertTransactionOriginatorDataAfterRollback(c4, k1, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testConditionalReplaceFromNonOwner() throws Exception {
      // we need 2 keys that reside on different caches...
      MagicKey k1 = new MagicKey("k1", c1, c2); // maps on to c1 and c2
      MagicKey k2 = new MagicKey("k2", c2, c3); // maps on to c2 and c3

      init(k1, k2);

      TransactionManager tm4 = getTransactionManager(c4);
      tm4.begin();
      assertRetVal(false, c4.replace(k1, "valueX", "new_value"));
      assertRetVal(false, c4.replace(k2, "valueX", "new_value"));

      assertEquals("value1", c4.get(k1));
      assertEquals("value2", c4.get(k2));

      assertRetVal(true, c4.replace(k1, "value1", "new_value"));
      assertRetVal(true, c4.replace(k2, "value2", "new_value"));

      assertEquals("new_value", c4.get(k1));
      assertEquals("new_value", c4.get(k2));
      tm4.rollback();

      assertIsInContainerImmortal(c1, k1);
      assertIsInContainerImmortal(c2, k1);
      assertIsInContainerImmortal(c2, k2);
      assertIsInContainerImmortal(c3, k2);

      //conditional operation always fetch entry from remote.
      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsNotInL1(c1, k2);
      assertIsNotInL1(c3, k1);

      checkOwnership(k1, k2, "value1", "value2");
   }

   public void testMergeFromNonOwner() {
      initAndTest();


      // merge function applied
      Object retval = getFirstNonOwner("k1").merge("k1", "value2", (v1, v2) -> "merged_" + v1 + "_" + v2);
      asyncWait("k1", ReadWriteKeyCommand.class);
      if (testRetVals) assertEquals("merged_value_value2", retval);
      assertOnAllCachesAndOwnership("k1", "merged_value_value2");

   }

   @Override
   protected ConfigurationBuilder buildConfiguration() {
      ConfigurationBuilder builder = super.buildConfiguration();
      ControlledConsistentHashFactory.Default chf = new ControlledConsistentHashFactory.Default(
            new int[][]{{0, 1}, {1, 2}});
      builder.clustering().hash().numOwners(2).numSegments(2);
      builder.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(chf);
      return builder;
   }

   private void checkOwnership(MagicKey k1, MagicKey k2, String v1, String v2) {
      assertOnAllCachesAndOwnership(k1, v1);
      assertOnAllCachesAndOwnership(k2, v2);

      assertIsInL1(c4, k1);
      assertIsInL1(c4, k2);
      assertIsInL1(c1, k2);
      assertIsInL1(c3, k1);
   }

   private void assertNotLocked(Cache c, Object... keys) {
      LockManager lm = extractComponent(c, LockManager.class);
      for (Object key : keys) {
         //the keys are unlocked asynchronously
         eventually(() -> format("Expected unlocked key '%s' (lock-owner='%s')", key, lm.getOwner(key)),
               () -> !lm.isLocked(key));
      }
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

   private <T> void assertRetVal(T expected, T retVal) {
      if (testRetVals) {
         assertEquals(expected, retVal);
      }
   }

   private void assertTransactionOriginatorDataAfterRollback(Cache<Object, String> cache, MagicKey k1, MagicKey k2) {
      if (testRetVals) {
         //entry is fetched and stored in L1 even if the TX rollbacks
         assertIsInL1(cache, k1);
         assertIsInL1(cache, k2);
      } else {
         //unsafe is enabled and the entry isn't fetched remotely.
         assertIsNotInL1(cache, k1);
         assertIsNotInL1(cache, k2);
      }
   }
}
