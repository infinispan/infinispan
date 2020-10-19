package org.infinispan.xsite.irac;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TestOperation;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.IsolationLevel;
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
      List<Irac3SitesConflictTest> tests = new ArrayList<>();
      for (ConfigMode configMode : ConfigMode.values()) {
         tests.add(new Irac3SitesConflictTest().configMode(configMode));
      }
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
      doTest(method, new TestOperationInterop(TestOperation.PUT_IF_ABSENT));
   }

   public void testPut(Method method) {
      doTest(method, new TestOperationInterop(TestOperation.PUT));
   }

   public void testReplace(Method method) {
      doTest(method, new TestOperationInterop(TestOperation.REPLACE));
   }

   public void testConditionalReplace(Method method) {
      doTest(method, new TestOperationInterop(TestOperation.REPLACE_CONDITIONAL));
   }

   public void testRemove(Method method) {
      doTest(method, new TestOperationInterop(TestOperation.REMOVE));
   }

   public void testConditionalRemove(Method method) {
      doTest(method, new TestOperationInterop(TestOperation.REMOVE_CONDITIONAL));
   }

   // TODO: need to do this still?
   //   public void testMaxIdleExpirationASync(Method method) {
//      doTest(method, TestOperation.REMOVE_MAX_IDLE_EXPIRED_ASYNC);
//   }
//
   public void testMaxIdleExpirationSync(Method method) {
      doTest(method, new RemoveExpiredOperation(replaceTimeService()));
   }

   private ControlledTimeService replaceTimeService() {
      ControlledTimeService timeService = new ControlledTimeService();
      for (int i = 0; i < N_SITES; i++) {
         String siteName = siteName(i);
         for (int j = 0; j < CLUSTER_SIZE; ++j) {
            Cache<?, ?> c = cache(siteName, j);
            // Max idle requires all caches to show it as expired to be removed.
            TestingUtil.replaceComponent(c.getCacheManager(), TimeService.class, timeService, true);
         }
      }

      return timeService;
   }

   interface IracTestOperation {
      <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue);

      boolean isRemove();

      <K, V> V insertValueAndReturnIfRequired(Cache<K, V> cache, K key, V value);

      default <V> V getValueFromArray(V[] array) {
         return array[0];
      }
   }

   static class RemoveExpiredOperation implements IracTestOperation {
      private final ControlledTimeService timeService;

      RemoveExpiredOperation(ControlledTimeService timeService) {
         this.timeService = timeService;
      }

      @Override
      public <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue) {
         timeService.advance(TimeUnit.SECONDS.toMillis(10));
         CompletionStages.join(cache.getAdvancedCache().removeMaxIdleExpired(key, prevValue));
         return null;
      }

      @Override
      public boolean isRemove() {
         return true;
      }

      @Override
      public <K, V> V insertValueAndReturnIfRequired(Cache<K, V> cache, K key, V value) {
         cache.put(key, value, -1, TimeUnit.SECONDS, 5, TimeUnit.SECONDS);
         return value;
      }

      @Override
      public <V> V getValueFromArray(V[] array) {
         // Our final value is a remove, however the put should win!
         return array[1];
      }
   }

   static class TestOperationInterop implements IracTestOperation {
      private final TestOperation testOperation;

      TestOperationInterop(TestOperation testOperation) {
         this.testOperation = testOperation;
      }

      @Override
      public <K, V> V execute(Cache<K, V> cache, K key, V prevValue, V newValue) {
         return testOperation.execute(cache, key, prevValue, newValue);
      }

      @Override
      public boolean isRemove() {
         return TestOperation.REMOVE == testOperation || TestOperation.REMOVE_CONDITIONAL == testOperation;
      }

      @Override
      public <K, V> V insertValueAndReturnIfRequired(Cache<K, V> cache, K key, V value) {
         if (testOperation.requiresPreviousValue()) {
            cache.put(key, value);
            return value;
         }
         return null;
      }
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

   private void doTest(Method method, IracTestOperation testConfig) {
      final String key = TestingUtil.k(method, 0);
      final String initialValue = testConfig.insertValueAndReturnIfRequired(cache(siteName(0), 0), key,
            TestingUtil.v(method, 0));

      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(initialValue, cache.get(key)));

      //disable xsite so each site won't send anything to the others
      iracManagerList.forEach(ManualIracManager::enable);

      //put a conflict value. each site has a different value for the same key
      String[] finalValues = new String[N_SITES];
      for (int i = 0; i < N_SITES; ++i) {
         String newValue = TestingUtil.v(method, (i + 1) * 2);
         if (testConfig.isRemove() && i > 0) {
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

      String expectedFinalValue = testConfig.getValueFromArray(finalValues);
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(expectedFinalValue, cache.get(key)));
   }
}
