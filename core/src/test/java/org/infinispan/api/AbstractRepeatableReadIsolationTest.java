package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional")
public abstract class AbstractRepeatableReadIsolationTest extends MultipleCacheManagersTest {

   private static final String INITIAL_VALUE = "init";
   private static final String FINAL_VALUE = "final";
   private static final String OTHER_VALUE = "other";
   private final CacheMode cacheMode;
   private final LockingMode lockingMode;

   protected AbstractRepeatableReadIsolationTest(CacheMode cacheMode, LockingMode lockingMode) {
      this.cacheMode = cacheMode;
      this.lockingMode = lockingMode;
   }

   public void testPutTxIsolationInOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(true, true, Operation.PUT);
   }

   public void testPutTxIsolationInOwnerWithKeyNoInitialized() throws Exception {
      doIsolationTest(true, false, Operation.PUT);
   }

   public void testPutTxIsolationInNonOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(false, true, Operation.PUT);
   }

   public void testPutTxIsolationInNonOwnerWithKeyNonInitialized() throws Exception {
      doIsolationTest(false, false, Operation.PUT);
   }

   public void testRemoveTxIsolationInOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(true, true, Operation.REMOVE);
   }

   public void testRemoveTxIsolationInOwnerWithKeyNoInitialized() throws Exception {
      doIsolationTest(true, false, Operation.REMOVE);
   }

   public void testRemoveTxIsolationInNonOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(false, true, Operation.REMOVE);
   }

   public void testRemoveTxIsolationInNonOwnerWithKeyNonInitialized() throws Exception {
      doIsolationTest(false, false, Operation.REMOVE);
   }

   public void testReplaceTxIsolationInOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(true, true, Operation.REPLACE);
   }

   public void testReplaceTxIsolationInOwnerWithKeyNoInitialized() throws Exception {
      doIsolationTest(true, false, Operation.REPLACE);
   }

   public void testReplaceTxIsolationInNonOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(false, true, Operation.REPLACE);
   }

   public void testReplaceTxIsolationInNonOwnerWithKeyNonInitialized() throws Exception {
      doIsolationTest(false, false, Operation.REPLACE);
   }

   public void testConditionalPutTxIsolationInOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(true, true, Operation.CONDITIONAL_PUT);
   }

   public void testConditionalPutTxIsolationInOwnerWithKeyNoInitialized() throws Exception {
      doIsolationTest(true, false, Operation.CONDITIONAL_PUT);
   }

   public void testConditionalPutTxIsolationInNonOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(false, true, Operation.CONDITIONAL_PUT);
   }

   public void testConditionalPutTxIsolationInNonOwnerWithKeyNonInitialized() throws Exception {
      doIsolationTest(false, false, Operation.CONDITIONAL_PUT);
   }

   public void testConditionalRemoveTxIsolationInOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(true, true, Operation.CONDITIONAL_REMOVE);
   }

   public void testConditionalRemoveTxIsolationInOwnerWithKeyNoInitialized() throws Exception {
      doIsolationTest(true, false, Operation.CONDITIONAL_REMOVE);
   }

   public void testConditionalRemoveTxIsolationInNonOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(false, true, Operation.CONDITIONAL_REMOVE);
   }

   public void testConditionalRemoveTxIsolationInNonOwnerWithKeyNonInitialized() throws Exception {
      doIsolationTest(false, false, Operation.CONDITIONAL_REMOVE);
   }

   public void testConditionalReplaceTxIsolationInOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(true, true, Operation.CONDITIONAL_REPLACE);
   }

   public void testConditionalReplaceTxIsolationInOwnerWithKeyNoInitialized() throws Exception {
      doIsolationTest(true, false, Operation.CONDITIONAL_REPLACE);
   }

   public void testConditionalReplaceTxIsolationInNonOwnerWithKeyInitialized() throws Exception {
      doIsolationTest(false, true, Operation.CONDITIONAL_REPLACE);
   }

   public void testConditionalReplaceTxIsolationInNonOwnerWithKeyNonInitialized() throws Exception {
      doIsolationTest(false, false, Operation.CONDITIONAL_REPLACE);
   }

   public void testPutTxIsolationAfterRemoveInOwner() throws Exception {
      doIsolationTestAfterRemove(true, Operation.PUT);
   }

   public void testPutTxIsolationAfterRemoveInNonOwner() throws Exception {
      doIsolationTestAfterRemove(false, Operation.PUT);
   }

   public void testRemoveTxIsolationAfterRemoveInOwner() throws Exception {
      doIsolationTestAfterRemove(true, Operation.REMOVE);
   }

   public void testRemoveTxIsolationAfterRemoveInNonOwner() throws Exception {
      doIsolationTestAfterRemove(false, Operation.REMOVE);
   }

   public void testReplaceTxIsolationAfterRemoveInOwner() throws Exception {
      doIsolationTestAfterRemove(true, Operation.REPLACE);
   }

   public void testReplaceTxIsolationAfterRemoveInNonOwner() throws Exception {
      doIsolationTestAfterRemove(false, Operation.REPLACE);
   }

   public void testConditionalPutTxIsolationAfterRemoveInOwner() throws Exception {
      doIsolationTestAfterRemove(true, Operation.CONDITIONAL_PUT);
   }

   public void testConditionalPutTxIsolationAfterRemoveInNonOwner() throws Exception {
      doIsolationTestAfterRemove(false, Operation.CONDITIONAL_PUT);
   }

   public void testConditionalRemoveTxIsolationAfterRemoveInOwner() throws Exception {
      doIsolationTestAfterRemove(true, Operation.CONDITIONAL_REMOVE);
   }

   public void testConditionalRemoveTxIsolationAfterRemoveInNonOwner() throws Exception {
      doIsolationTestAfterRemove(false, Operation.CONDITIONAL_REMOVE);
   }

   public void testConditionalReplaceTxIsolationAfterRemoveInOwner() throws Exception {
      doIsolationTestAfterRemove(true, Operation.CONDITIONAL_REPLACE);
   }

   public void testConditionalReplaceTxIsolationAfterRemoveInNonOwner() throws Exception {
      doIsolationTestAfterRemove(false, Operation.CONDITIONAL_REPLACE);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, true);
      builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(false);
      builder.transaction().lockingMode(lockingMode);
      builder.clustering().hash().numOwners(1);
      createClusteredCaches(2, builder);
   }

   private void doIsolationTest(boolean executeOnOwner, boolean initialized, Operation operation) throws Exception {
      final Cache<Object, Object> ownerCache = cache(0);
      final Object key = new MagicKey("shared", ownerCache);
      final Cache<Object, Object> cache = executeOnOwner ? cache(0) : cache(1);
      final TransactionManager tm = executeOnOwner ? tm(0) : tm(1);

      assertValueInAllCaches(key, null);

      final Object initValue = initialized ? INITIAL_VALUE : null;
      if (initialized) {
         ownerCache.put(key, initValue);
         assertValueInAllCaches(key, initValue);
      }

      tm.begin();
      assertEquals("Wrong first get.", initValue, cache.get(key));
      Transaction tx = tm.suspend();

      ownerCache.put(key, OTHER_VALUE);
      assertValueInAllCaches(key, OTHER_VALUE);

      Object finalValueExpected = null;

      tm.resume(tx);
      assertEquals("Wrong second get.", initValue, cache.get(key));
      switch (operation) {
         case PUT:
            finalValueExpected = FINAL_VALUE;
            assertEquals("Wrong put return value.", initValue, cache.put(key, FINAL_VALUE));
            assertEquals("Wrong final get.", FINAL_VALUE, cache.get(key));
            break;
         case REMOVE:
            finalValueExpected = null;
            assertEquals("Wrong remove return value.", initValue, cache.remove(key));
            assertEquals("Wrong final get.", null, cache.get(key));
            break;
         case REPLACE:
            finalValueExpected = initialized ? FINAL_VALUE : OTHER_VALUE;
            assertEquals("Wrong replace return value.", initValue, cache.replace(key, FINAL_VALUE));
            assertEquals("Wrong final get.", initialized ? FINAL_VALUE : initValue, cache.get(key));
            break;
         case CONDITIONAL_PUT:
            finalValueExpected = initialized ? OTHER_VALUE : FINAL_VALUE;
            assertEquals("Wrong put return value.", initialized ? initValue : null, cache.putIfAbsent(key, FINAL_VALUE));
            assertEquals("Wrong final get.", initialized ? initValue : FINAL_VALUE, cache.get(key));
            break;
         case CONDITIONAL_REMOVE:
            finalValueExpected = initialized ? null : OTHER_VALUE;
            assertEquals("Wrong remove return value.", initialized, cache.remove(key, INITIAL_VALUE));
            assertEquals("Wrong final get.", null, cache.get(key));
            break;
         case CONDITIONAL_REPLACE:
            finalValueExpected = initialized ? FINAL_VALUE : OTHER_VALUE;
            assertEquals("Wrong replace return value.", initialized, cache.replace(key, INITIAL_VALUE, FINAL_VALUE));
            assertEquals("Wrong final get.", initialized ? FINAL_VALUE : initValue, cache.get(key));
            break;
         default:
            fail("Unknown operation " + operation);
            break;
      }
      tm.commit();

      assertValueInAllCaches(key, finalValueExpected);
      assertNoTransactions();
   }

   private void doIsolationTestAfterRemove(boolean executeOnOwner, Operation operation) throws Exception {
      final Cache<Object, Object> ownerCache = cache(0);
      final Object key = new MagicKey("shared", ownerCache);
      final Cache<Object, Object> cache = executeOnOwner ? cache(0) : cache(1);
      final TransactionManager tm = executeOnOwner ? tm(0) : tm(1);

      assertValueInAllCaches(key, null);

      final Object initValue = INITIAL_VALUE;
      ownerCache.put(key, initValue);
      assertValueInAllCaches(key, initValue);


      tm.begin();
      assertEquals("Wrong first get.", initValue, cache.get(key));
      Transaction tx = tm.suspend();

      ownerCache.put(key, OTHER_VALUE);
      assertValueInAllCaches(key, OTHER_VALUE);

      Object finalValueExpected = null;

      tm.resume(tx);
      assertEquals("Wrong second get.", initValue, cache.get(key));
      assertEquals("Wrong value after remove.", initValue, cache.remove(key));
      switch (operation) {
         case PUT:
            finalValueExpected = FINAL_VALUE;
            assertEquals("Wrong put return value.", null, cache.put(key, FINAL_VALUE));
            assertEquals("Wrong final get.", FINAL_VALUE, cache.get(key));
            break;
         case REMOVE:
            finalValueExpected = null;
            assertEquals("Wrong remove return value.", null, cache.remove(key));
            assertEquals("Wrong final get.", null, cache.get(key));
            break;
         case REPLACE:
            finalValueExpected = null;
            assertEquals("Wrong replace return value.", null, cache.replace(key, FINAL_VALUE));
            assertEquals("Wrong final get.", null, cache.get(key));
            break;
         case CONDITIONAL_PUT:
            finalValueExpected = FINAL_VALUE;
            assertEquals("Wrong put return value.", null, cache.putIfAbsent(key, FINAL_VALUE));
            assertEquals("Wrong final get.", FINAL_VALUE, cache.get(key));
            break;
         case CONDITIONAL_REMOVE:
            finalValueExpected = null;
            assertEquals("Wrong remove return value.", false, cache.remove(key, INITIAL_VALUE));
            assertEquals("Wrong final get.", null, cache.get(key));
            break;
         case CONDITIONAL_REPLACE:
            finalValueExpected = null;
            assertEquals("Wrong replace return value.", false, cache.replace(key, INITIAL_VALUE, FINAL_VALUE));
            assertEquals("Wrong final get.", null, cache.get(key));
            break;
         default:
            fail("Unknown operation " + operation);
            break;
      }
      tm.commit();

      assertValueInAllCaches(key, finalValueExpected);
      assertNoTransactions();
   }

   private void assertValueInAllCaches(final Object key, final Object value) {
      for (Cache<Object, Object> cache : caches()) {
         assertEquals("Wrong value.", value, cache.get(key));
      }
   }

   private static enum Operation {
      PUT, REMOVE, REPLACE,
      CONDITIONAL_PUT, CONDITIONAL_REMOVE, CONDITIONAL_REPLACE
   }
}
