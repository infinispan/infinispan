package org.infinispan.xsite;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.testng.AssertJUnit.assertNull;

import java.util.LinkedList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author William Burns
 * @since 12.0
 */
@Test(groups = "xsite", testName = "xsite.AsyncBackupExpirationTest")
public class AsyncBackupRemoveTest extends AbstractTwoSitesTest {

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
//      };
      List<AsyncBackupRemoveTest> tests = new LinkedList<>();
      tests.add(new AsyncBackupRemoveTest().setLonConfigMode(ConfigMode.NON_TX).setNycConfigMode(ConfigMode.NON_TX));
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

   public AsyncBackupRemoveTest() {
      super.lonBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
      super.nycBackupStrategy = BackupConfiguration.BackupStrategy.ASYNC;
      super.implicitBackupCache = true;
   }

   @Override
   protected ConfigurationBuilder getLonActiveConfig() {
      return getConfig(lonConfigMode);
   }

   private AsyncBackupRemoveTest setLonConfigMode(ConfigMode configMode) {
      this.lonConfigMode = configMode;
      return this;
   }

   private AsyncBackupRemoveTest setNycConfigMode(ConfigMode configMode) {
      this.nycConfigMode = configMode;
      return this;
   }

   private ControlledTimeService replaceTimeService() {
      ControlledTimeService timeService = new ControlledTimeService();
      // Max idle requires all caches to show it as expired to be removed.
      for (Cache<?, ?> c : caches(LON)) {
         replaceComponent(c.getCacheManager(), TimeService.class, timeService, true);
      }

      for (Cache<?, ?> c : caches(NYC)) {
         replaceComponent(c.getCacheManager(), TimeService.class, timeService, true);
      }

      return timeService;
   }


   public void testMaxIdleWithRecentAccess() {
      Cache<Object, Object> mainSiteCache = cache(LON, 0);
      Cache<Object, Object> backupSiteCache = cache(NYC, 0);

      Object key = new MagicKey(cache(LON, 1));
      String value = "v";

      mainSiteCache.put(key, value);

      // Wait for the value to be propagated to the xsite
      eventuallyEquals(value, () -> backupSiteCache.get(key));

      mainSiteCache.remove(key);

      eventuallyEquals(value, () -> backupSiteCache.get(key));

      try {
         Thread.sleep(5000);
      } catch (InterruptedException e) {
         e.printStackTrace();
      }

      assertNull(TestingUtil.extractComponent(cache(LON, 1), IracVersionGenerator.class).getTombstone(key));
      assertNull(TestingUtil.extractComponent(cache(NYC, 1), IracVersionGenerator.class).getTombstone(key));
      assertNull(TestingUtil.extractComponent(backupSiteCache, IracVersionGenerator.class).getTombstone(key));
      assertNull(TestingUtil.extractComponent(mainSiteCache, IracVersionGenerator.class).getTombstone(key));
   }

   private enum ConfigMode {
      NON_TX,
      PESSIMISTIC_TX,
      OPTIMISTIC_TX_RC,
      OPTIMISTIC_TX_RR,
   }
}
