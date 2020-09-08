package org.infinispan.xsite.irac;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.TestOperation;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * Tests if the simple conflict resolution works with 3 sites.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Test(groups = "functional", testName = "xsite.irac.Irac3SitesConflictTest")
public class Irac3SitesConflictTest extends AbstractMultipleSitesTest {
   private static final int N_SITES = 3;
   private static final int CLUSTER_SIZE = 3;
   private final List<ManualIracManager> iracManagerList;

   private ConfigMode configMode;

   public Irac3SitesConflictTest configMode(ConfigMode configMode) {
      this.configMode = configMode;
      return this;
   }

   private enum ConfigMode {
      NON_TX,
      PESSIMISTIC_TX,
      OPTIMISTIC_TX_RC,
      OPTIMISTIC_TX_RR,
   }

   @Factory
   public Object[] factory() {
      List<Irac3SitesConflictTest> tests = new LinkedList<>();
//      for (ConfigMode configMode : ConfigMode.values()) {
//         tests.add(new Irac3SitesConflictTest().configMode(configMode));
//      }
      tests.add(new Irac3SitesConflictTest().configMode(ConfigMode.PESSIMISTIC_TX));
      return tests.toArray();
   }

   @Override
   protected String[] parameterNames() {
      return new String[]{"configMode"};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{configMode};
   }

   protected Irac3SitesConflictTest() {
      this.iracManagerList = new ArrayList<>(N_SITES * CLUSTER_SIZE);
   }

   public void testPutIfAbsent(Method method) {
      doTest(method, TestOperation.PUT_IF_ABSENT);
   }

   public void testPut(Method method) {
      doTest(method, TestOperation.PUT);
   }

   public void testReplace(Method method) {
      doTest(method, TestOperation.REPLACE);
   }

   public void testConditionalReplace(Method method) {
      doTest(method, TestOperation.REPLACE_CONDITIONAL);
   }

   public void testRemove(Method method) {
      doTest(method, TestOperation.REMOVE);
   }

   public void testMaxIdleExpirationSync(Method method) {
      doTest(method, Operations.REMOVE_MAX_IDLE_EXPIRED_SYNC);
   }

   public void testMaxIdleExpirationASync(Method method) {
      doTest(method, Operations.REMOVE_MAX_IDLE_EXPIRED_ASYNC);
   }

   public void testConditionalRemove(Method method) {
      doTest(method, TestOperation.REMOVE_CONDITIONAL);
   }

   @Override
   protected int defaultNumberOfSites() {
      return N_SITES;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return CLUSTER_SIZE;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, configMode != ConfigMode.NON_TX);
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
      for (int i = 0; i < N_SITES; ++i) {
         if (i == siteIndex) {
            //don't add our site as backup.
            continue;
         }
         builder.sites()
               .addBackup()
               .site(siteName(i))
               .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      }
      return builder;
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationForSite(int siteIndex) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
      return builder;
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      iracManagerList.forEach(iracManager -> iracManager.disable(ManualIracManager.DisableMode.DROP));
      super.clearContent();
   }

   @Override
   protected void afterSitesCreated() {
      for (int i = 0; i < N_SITES; ++i) {
         for (Cache<?, ?> cache : caches(siteName(i))) {
            iracManagerList.add(ManualIracManager.wrapCache(cache));
         }
      }
   }

   private void doTest(Method method, TestOperation testConfig) {
      final String key = TestingUtil.k(method, 0);
      final String initialValue = testConfig.requiresPreviousValue() ? TestingUtil.v(method, 0) : null;

      //init cache if needed!
      if (testConfig.requiresPreviousValue()) {
         cache(siteName(0), 0).put(key, initialValue);
      }
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(initialValue, cache.get(key)));

      //disable xsite so each site won't send anything to the others
      iracManagerList.forEach(ManualIracManager::enable);

      //put a conflict value. each site has a different value for the same key
      String[] finalValues = new String[N_SITES];
      for (int i = 0; i < N_SITES; ++i) {
         String newValue = TestingUtil.v(method, (i + 1) * 2);
         if ((testConfig == TestOperation.REMOVE_CONDITIONAL || testConfig == TestOperation.REMOVE) && i > 0) {
            //to make sure remove works, we remove from LON only since it is the winning site.
            //the other sites put other value.
            cache(siteName(i), 0).put(key, newValue);
            finalValues[i] = newValue;
         } else {
            finalValues[i] = testConfig.execute(cache(siteName(i), 0), key, initialValue, newValue);
         }
      }

      //check if everything is correct
      for (int i = 0; i < N_SITES; ++i) {
         String fValue = finalValues[i];
         assertInSite(siteName(i), cache -> AssertJUnit.assertEquals(fValue, cache.get(key)));
      }

      //enable xsite. this will send the keys!
      iracManagerList.forEach(manualIracManager -> manualIracManager.disable(ManualIracManager.DisableMode.SEND));

      String expectedFinalValue = finalValues[0];
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(expectedFinalValue, cache.get(key)));
   }
}
