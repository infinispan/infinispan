package org.infinispan.xsite;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author William Burns
 * @since 12.0
 */
@Test(groups = "xsite", testName = "xsite.AsyncBackupExpirationTest")
public class AsyncBackupExpirationTest extends AbstractTwoSitesTest {

   private ConfigMode lonConfigMode;
   private ConfigMode nycConfigMode;

   private static ConfigurationBuilder getConfig(ConfigMode configMode) {
      if (configMode == ConfigMode.NON_TX) {
         return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      }
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      switch (configMode) {
         case OPTIMISTIC_TX_RC:
            builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
            builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
            break;
         case OPTIMISTIC_TX_RR:
            builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
            builder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
            break;
         case PESSIMISTIC_TX:
            builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
            break;
      }
      builder.expiration().wakeUpInterval(-1);
      return builder;
   }

   @Override
   protected GlobalConfigurationBuilder globalConfigurationBuilderForSite(String siteName) {
      return super.globalConfigurationBuilderForSite(siteName);
   }

   @Factory
   public Object[] factory() {
      List<AsyncBackupExpirationTest> tests = new LinkedList<>();
      for (ConfigMode lon : ConfigMode.values()) {
         for (ConfigMode nyc : ConfigMode.values()) {
            tests.add(new AsyncBackupExpirationTest().setLonConfigMode(lon).setNycConfigMode(nyc));
         }
      }
      return tests.toArray();
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"LON", "NYC"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{lonConfigMode, nycConfigMode};
   }

   @Override
   protected ConfigurationBuilder getNycActiveConfig() {
      return getConfig(nycConfigMode);
   }

   @BeforeMethod
   public void ensureSitesOnline() {
      // Now we take the backup offline - which should refresh our access times
      XSiteAdminOperations adminOperations = extractComponent(cache(LON, 0), XSiteAdminOperations.class);
      if (XSiteAdminOperations.OFFLINE.equals(adminOperations.siteStatus(NYC))) {
         adminOperations.bringSiteOnline(NYC);
      }

      adminOperations = extractComponent(cache(NYC, 0), XSiteAdminOperations.class);
      if (XSiteAdminOperations.OFFLINE.equals(adminOperations.siteStatus(LON))) {
         adminOperations.bringSiteOnline(LON);
      }
   }

   public AsyncBackupExpirationTest() {
      super.lonBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
      super.nycBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
      super.implicitBackupCache = true;
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getConfig(lonConfigMode);
   }

   private AsyncBackupExpirationTest setLonConfigMode(ConfigMode configMode) {
      this.lonConfigMode = configMode;
      return this;
   }

   private AsyncBackupExpirationTest setNycConfigMode(ConfigMode configMode) {
      this.nycConfigMode = configMode;
      return this;
   }

   @DataProvider(name = "two boolean cross product")
   public Object[][] tx() {
      return new Object[][]{
            {false, false},
            {false, true},
            {true, false},
            {true, true}
      };
   }

   private ControlledTimeService replaceTimeService() {
      ControlledTimeService timeService = new ControlledTimeService();
      // Max idle requires all caches to show it as expired to be removed.
      for (Cache<?, ?> c : caches(LON)) {
         TestingUtil.replaceComponent(c.getCacheManager(), TimeService.class, timeService, true);
      }

      for (Cache<?, ?> c : caches(NYC)) {
         TestingUtil.replaceComponent(c.getCacheManager(), TimeService.class, timeService, true);
      }

      return timeService;
   }

   @Test(dataProvider = "two boolean cross product")
   public void testExpiredAccess(boolean lifespan, boolean readOnPrimary) throws InterruptedException {
      Cache<MagicKey, String> cache = cache(LON, 0);

      ControlledTimeService timeService = replaceTimeService();

      MagicKey key = readOnPrimary ? new MagicKey(cache) : new MagicKey(cache(LON, 1));
      if (lifespan) {
         cache.put(key, "v", 1, TimeUnit.SECONDS);
      } else {
         cache.put(key, "v", -1, TimeUnit.SECONDS, 1, TimeUnit.SECONDS);
      }
      eventuallyEquals("v", () -> cache(LON, 0).get(key));
      eventuallyEquals("v", () -> cache(LON, 1).get(key));
      Cache<Object, Object> backupCache = backup(LON);
      assertNull(backupCache.get("k"));
      eventuallyEquals("v", () -> backup(LON).get(key));

      // Now expire the entry
      timeService.advance(TimeUnit.SECONDS.toMillis(2));
      assertNull(cache.get(key));

      // TODO: check for the touch command being invoked on the remote site for lifespan = false
   }

   @Test(dataProvider = "two boolean cross product")
   public void testMaxIdleWithRecentAccess(boolean readFromWrittenSite, boolean readOnAccessedSite) throws InterruptedException {
      Cache<Object, Object> mainSiteCache = cache(LON, 0);
      Cache<Object, Object> backupSiteCache = cache(NYC, 0);

      ControlledTimeService timeService = replaceTimeService();

      Object key = new MagicKey(cache(LON, 1));
      String value = "v";

      long accessTime = 10;

      mainSiteCache.put(key, value, -1, TimeUnit.SECONDS, accessTime, TimeUnit.MILLISECONDS);

      // Wait for the value to be propagated to the xsite
      eventuallyEquals(value, () -> backupSiteCache.get(key));

      // Just before it expires we read the key from a site
      timeService.advance(accessTime - 1);

      Cache<Object, Object> readSite = readFromWrittenSite ? mainSiteCache : backupSiteCache;
      Cache<Object, Object> expiredSite = readFromWrittenSite ? backupSiteCache : mainSiteCache;

      assertEquals(value, readSite.get(key));

      // Now advance it and it should be "expired" from the non read site, but the other isn't
      timeService.advance(accessTime - 1);

      if (readOnAccessedSite) {
         assertEquals(value, readSite.get(key));
      } else {
         assertEquals(value, expiredSite.get(key));
      }

      // Now this will be expired on both nodes
      timeService.advance(accessTime + 1);

      assertNull(readSite.get(key));
      assertNull(expiredSite.get(key));
   }

   private void takeBothOffline() {
      // Now we take the backup offline - which should refresh our access times
      XSiteAdminOperations adminOperations = extractComponent(cache(LON, 0), XSiteAdminOperations.class);
      adminOperations.takeSiteOffline(NYC);

      adminOperations = extractComponent(cache(NYC, 0), XSiteAdminOperations.class);
      adminOperations.takeSiteOffline(LON);
   }

   @Test(dataProvider = "two boolean cross product")
   public void testAccessButSiteGoesDown(boolean readFromPrimary, boolean readFromPrimaryAfterTakeOffline) {
      Cache<Object, Object> mainSiteCache = cache(LON, 0);
      Cache<Object, Object> backupSiteCache = cache(NYC, 0);

      ControlledTimeService timeService = replaceTimeService();

      Object key = "key";
      String value = "v";

      long accessTime = 10;

      mainSiteCache.put(key, value, -1, TimeUnit.SECONDS, accessTime, TimeUnit.MILLISECONDS);

      // Wait for the value to be propagated to the xsite
      eventuallyEquals(value, () -> backupSiteCache.get(key));

      // Just before it expires we read the key from a site
      timeService.advance(accessTime - 1);

      assertEquals(value, readFromPrimary ? mainSiteCache.get(key) : backupSiteCache.get(key));

      takeBothOffline();

      // This will expire the entry if the access time wasn't refreshed from the other site being taken offline
      timeService.advance(accessTime - 1);

      assertEquals(value, readFromPrimaryAfterTakeOffline ? mainSiteCache.get(key) : backupSiteCache.get(key));
   }

   private enum ConfigMode {
      NON_TX,
      PESSIMISTIC_TX,
      OPTIMISTIC_TX_RC,
      OPTIMISTIC_TX_RR,
   }
}
