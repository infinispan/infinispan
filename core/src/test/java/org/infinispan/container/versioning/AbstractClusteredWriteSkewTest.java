package org.infinispan.container.versioning;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

@Test(testName = "container.versioning.AbstractClusteredWriteSkewTest", groups = "functional")
public abstract class AbstractClusteredWriteSkewTest extends MultipleCacheManagersTest {


   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      builder
            .clustering()
            .cacheMode(getCacheMode())
            .versioning()
            .enable()
            .scheme(VersioningScheme.SIMPLE)
            .locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true)
            .transaction()
            .lockingMode(LockingMode.OPTIMISTIC)
            .syncCommitPhase(true);

      decorate(builder);

      createCluster(builder, clusterSize());
      waitForClusterToForm();
   }

   protected void decorate(ConfigurationBuilder builder) {
      // No-op
   }

   protected abstract CacheMode getCacheMode();

   protected abstract int clusterSize();

   public final void testPutIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.PUT, false);
   }

   public final void testPutIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.PUT, false);
   }

   public final void testPutIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.PUT, true);
   }

   public final void testPutIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.PUT, true);
   }

   public final void testRemoveIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REMOVE, false);
   }

   public final void testRemoveIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REMOVE, false);
   }

   public final void testRemoveIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REMOVE, true);
   }

   public final void testRemoveIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REMOVE, true);
   }

   public final void testReplaceIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REPLACE, false);
   }

   public final void testReplaceIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REPLACE, false);
   }

   public final void testReplaceIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.REPLACE, true);
   }

   public final void testReplaceIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.REPLACE, true);
   }

   public final void testPutIfAbsentIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_PUT, false);
   }

   public final void testPutIfAbsentIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_PUT, false);
   }

   public final void testPutIfAbsentIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_PUT, true);
   }

   public final void testPutIfAbsentIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_PUT, true);
   }

   public final void testConditionalRemoveIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REMOVE, false);
   }

   public final void testConditionalRemoveIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REMOVE, false);
   }

   public final void testConditionalRemoveIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REMOVE, true);
   }

   public final void testConditionalRemoveIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REMOVE, true);
   }

   public final void testConditionalReplaceIgnoreReturnValueOnNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REPLACE, false);
   }

   public final void testConditionalReplaceIgnoreReturnValueOnNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REPLACE, false);
   }

   public final void testConditionalReplaceIgnoreReturnValueNonExistingKey() throws Exception {
      doIgnoreReturnValueTest(true, Operation.CONDITIONAL_REPLACE, true);
   }

   public final void testConditionalReplaceIgnoreReturnValueNonExistingKeyOnNonOwner() throws Exception {
      doIgnoreReturnValueTest(false, Operation.CONDITIONAL_REPLACE, true);
   }

   private void doIgnoreReturnValueTest(boolean executeOnPrimaryOwner, Operation operation, boolean initKey) throws Exception {
      final Object key = new MagicKey("ignore-return-value", cache(0));
      final AdvancedCache<Object, Object> c = executeOnPrimaryOwner ? advancedCache(0) : advancedCache(1);
      final TransactionManager tm = executeOnPrimaryOwner ? tm(0) : tm(1);

      for (Cache cache : caches()) {
         AssertJUnit.assertNull("wrong initial value for " + address(cache) + ".", cache.get(key));
      }

      log.debugf("Initialize the key? %s", initKey);
      if (initKey) {
         cache(0).put(key, "init");
      }

      Object finalValue = null;
      boolean rollbackExpected = false;

      log.debugf("Start the transaction and perform a %s operation", operation);
      tm.begin();
      switch (operation) {
         case PUT:
            finalValue = "v1";
            rollbackExpected = false;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).put(key, "v1");
            break;
         case REMOVE:
            finalValue = null;
            rollbackExpected = false;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).remove(key);
            break;
         case REPLACE:
            finalValue = "v2";
            rollbackExpected = initKey;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).replace(key, "v1");
            break;
         case CONDITIONAL_PUT:
            finalValue = "v2";
            rollbackExpected = !initKey;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).putIfAbsent(key, "v1");
            break;
         case CONDITIONAL_REMOVE:
            finalValue = "v2";
            rollbackExpected = initKey;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).remove(key, "init");
            break;
         case CONDITIONAL_REPLACE:
            finalValue = "v2";
            rollbackExpected = initKey;
            c.withFlags(Flag.IGNORE_RETURN_VALUES).replace(key, "init", "v1");
            break;
         default:
            tm.rollback();
            fail("Unknown operation " + operation);
      }
      Transaction tx = tm.suspend();

      log.debugf("Suspend the transaction and update the key");
      c.put(key, "v2");

      log.debugf("Checking if all the keys has the same value");
      for (Cache cache : caches()) {
         assertEquals("wrong intermediate value for " + address(cache) + ".", "v2", cache.get(key));
      }

      log.debugf("It is going to try to commit the suspended transaction");
      try {
         tm.resume(tx);
         tm.commit();
         if (rollbackExpected) {
            fail("Rollback expected!");
         }
      } catch (RollbackException e) {
         if (!rollbackExpected) {
            fail("Rollback *not* expected!");
         }
         //no-op
      }

      log.debugf("So far so good. Check the key final value");
      assertNoTransactions();
      for (Cache cache : caches()) {
         assertEquals("wrong final value for " + address(cache) + ".", finalValue, cache.get(key));
      }
   }

   private static enum Operation {
      PUT, REMOVE, REPLACE,
      CONDITIONAL_PUT, CONDITIONAL_REMOVE, CONDITIONAL_REPLACE
   }
}
