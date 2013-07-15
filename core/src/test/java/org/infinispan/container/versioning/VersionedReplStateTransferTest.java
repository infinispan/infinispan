package org.infinispan.container.versioning;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

@Test(testName = "container.versioning.VersionedReplStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class VersionedReplStateTransferTest extends MultipleCacheManagersTest {

   ConfigurationBuilder builder;

   @Override
   protected void createCacheManagers() throws Throwable {
      builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);

      builder
            .clustering()
               .cacheMode(CacheMode.REPL_SYNC)
            .versioning()
               .enable()
               .scheme(VersioningScheme.SIMPLE)
            .locking()
               .isolationLevel(IsolationLevel.REPEATABLE_READ)
               .writeSkewCheck(true)
            .transaction()
               .lockingMode(LockingMode.OPTIMISTIC)
               .syncCommitPhase(true);

      amendConfig(builder);

      createCluster(builder, 2);
      waitForClusterToForm();
   }

   protected void amendConfig(ConfigurationBuilder builder) {
   }

   public void testStateTransfer() throws Exception {
      Cache<Object, Object> cache0 = cache(0);
      Cache<Object, Object> cache1 = cache(1);

      cache0.put("hello", "world");

      assertEquals("world", cache0.get("hello"));
      assertEquals("world", cache1.get("hello"));

      tm(1).begin();
      assertEquals("world", cache1.get("hello"));
      Transaction t = tm(1).suspend();

      // create a cache2
      addClusterEnabledCacheManager(builder);
      Cache<Object, Object> cache2 = cache(2);

      assertEquals("world", cache2.get("hello"));

      cacheManagers.get(0).stop();
      cacheManagers.remove(0);
      waitForClusterToForm();

      // Cause a write skew
      cache2.put("hello", "new world");

      tm(1).resume(t);
      cache1.put("hello", "world2");

      try {
         tm(1).commit();
         fail("Write skew check should fail");
      } catch (RollbackException expected) {
         // Expected
      }

      assertEquals("new world", cache1.get("hello"));
      assertEquals("new world", cache2.get("hello"));
   }
}
