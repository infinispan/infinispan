package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Test that the default expiration parameters are set properly with clustered write skew checks enabled.
 *
 * See https://issues.jboss.org/browse/ISPN-7105
 */
@Test(groups = "functional", testName = "expiration.impl.ExpirationWriteSkewFunctionalTest")
public class ExpirationWithClusteredWriteSkewTest extends MultipleCacheManagersTest {
   public static final String KEY = "key";
   public static final String VALUE = "value";

   private ControlledTimeService timeService = new ControlledTimeService(0);
   private ExpirationManager expirationManager1;
   private ExpirationManager expirationManager2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ControlledConsistentHashFactory chf = new ControlledConsistentHashFactory.Default(0, 1);
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder
            .clustering()
            .cacheMode(CacheMode.REPL_SYNC)
            .hash()
            .numSegments(1)
            .consistentHashFactory(chf)
            .expiration()
            .lifespan(10, TimeUnit.SECONDS)
            .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .lockingMode(LockingMode.OPTIMISTIC)
            .locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ)
            .writeSkewCheck(true)
            .versioning()
            .enable()
            .scheme(VersioningScheme.SIMPLE);

      createCluster(builder, 2);
      TestingUtil.replaceComponent(manager(0), TimeService.class, timeService, true);
      expirationManager1 = TestingUtil.extractComponent(cache(0), ExpirationManager.class);
      TestingUtil.replaceComponent(manager(1), TimeService.class, timeService, true);
      expirationManager2 = TestingUtil.extractComponent(cache(1), ExpirationManager.class);
   }

   public void testDefaultExpirationInTransaction() throws Exception {
      Cache<Object, Object> cache0 = cache(0);

      tm(0).begin();
      assertNull(cache0.get(KEY));
      cache0.put(KEY, VALUE);
      CacheEntry entryInTx = cache0.getAdvancedCache().getCacheEntry(KEY);
      assertEquals(10000, entryInTx.getLifespan());
      tm(0).commit();

      CacheEntry entryAfterCommit = cache0.getAdvancedCache().getCacheEntry(KEY);
      assertEquals(10000, entryAfterCommit.getLifespan());

      timeService.advance(TimeUnit.SECONDS.toMillis(10) + 1);
      // Required since cache loader size calls to store - we have to make sure the store expires entries
      expirationManager1.processExpiration();
      expirationManager2.processExpiration();
      assertEquals(0, cache0.size());
   }
}
