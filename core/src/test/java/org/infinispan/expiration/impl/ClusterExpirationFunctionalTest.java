package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tests to make sure that when expiration occurs it occurs across the cluster
 *
 * @author William Burns
 * @since 8.0
 */
@Test(groups = "functional", testName = "expiration.impl.ClusterExpirationFunctionalTest")
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC, CacheMode.SCATTERED_SYNC})
public class ClusterExpirationFunctionalTest extends MultipleCacheManagersTest {

   protected final Log log = LogFactory.getLog(getClass());

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;
   protected ControlledTimeService ts2;

   protected Cache<Object, String> cache0;
   protected Cache<Object, String> cache1;
   protected Cache<Object, String> cache2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(cacheMode);
      createCluster(builder, 3);
      waitForClusterToForm();
      injectTimeServices();

      cache0 = cache(0);
      cache1 = cache(1);
      cache2 = cache(2);
   }

   protected void injectTimeServices() {
      long now = System.currentTimeMillis();
      ts0 = new ControlledTimeService(now);
      TestingUtil.replaceComponent(manager(0), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService(now);
      TestingUtil.replaceComponent(manager(1), TimeService.class, ts1, true);
      ts2 = new ControlledTimeService(now);
      TestingUtil.replaceComponent(manager(2), TimeService.class, ts2, true);
   }

   public void testExpiredOnPrimaryOwner() throws Exception {
      testExpiredEntryRetrieval(cache0, cache1, ts0, true);
   }

   public void testExpiredOnBackupOwner() throws Exception {
      testExpiredEntryRetrieval(cache0, cache1, ts1, false);
   }

   private void testExpiredEntryRetrieval(Cache<Object, String> primaryOwner, Cache<Object, String> backupOwner,
           ControlledTimeService timeService, boolean expireOnPrimary) throws Exception {
      MagicKey key = createKey(primaryOwner, backupOwner);
      primaryOwner.put(key, key.toString(), 10, TimeUnit.MINUTES);

      assertEquals(key.toString(), primaryOwner.get(key));
      assertEquals(key.toString(), backupOwner.get(key));

      // Now we expire on cache0, it should still exist on cache1
      timeService.advance(TimeUnit.MINUTES.toMillis(10) + 1);

      Cache<?, ?> expiredCache;
      Cache<?, ?> otherCache;
      if (expireOnPrimary) {
         expiredCache = primaryOwner;
         otherCache = backupOwner;
      } else {
         expiredCache = backupOwner;
         otherCache = primaryOwner;
      }

      Cache<?, ?> other;
      if (cacheMode.isScattered()) {
         // In scattered cache the read would go
         other = otherCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_OWNERSHIP_CHECK);
      } else {
         other = otherCache;
      }
      assertEquals(key.toString(), other.get(key));

      // By calling get on an expired key it will remove it all over
      Object expiredValue = expiredCache.get(key);
      // In scattered mode, the get goes from backup to remote node where the time has not passed yet,
      // therefore it is not expired. We don't check expiration on local node after receiving the response.
      if (cacheMode.isScattered() && !expireOnPrimary) {
         assertEquals(key.toString(), expiredValue);
      }  else {
         assertNull(expiredValue);
         // This should be expired on the other node soon - note expiration is done asynchronously on a get
         Eventually.eventually(() -> !other.containsKey(key), 10, TimeUnit.SECONDS);
      }
   }

   private MagicKey createKey(Cache<Object, String> primaryOwner, Cache<Object, String> backupOwner) {
      if (cacheMode.isScattered()) {
         return new MagicKey(primaryOwner);
      } else {
         return new MagicKey(primaryOwner, backupOwner);
      }
   }

   public void testExpiredOnBoth() {
      MagicKey key = createKey(cache0, cache1);
      cache0.put(key, key.toString(), 10, TimeUnit.MINUTES);

      assertEquals(key.toString(), cache0.get(key));
      assertEquals(key.toString(), cache1.get(key));

      // Now we expire on cache0, it should still exist on cache1
      ts0.advance(TimeUnit.MINUTES.toMillis(10) + 1);
      ts1.advance(TimeUnit.MINUTES.toMillis(10) + 1);

      // Both should be null
      assertNull(cache0.get(key));
      assertNull(cache1.get(key));
   }
}
