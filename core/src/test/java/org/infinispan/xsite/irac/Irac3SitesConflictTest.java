package org.infinispan.xsite.irac;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
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

   protected Irac3SitesConflictTest() {
      this.iracManagerList = new ArrayList<>(N_SITES * CLUSTER_SIZE);
   }

   public void testPutIfAbsent(Method method) {
      doTest(method, Operations.PUT_IF_ABSENT);
   }

   public void testPut(Method method) {
      doTest(method, Operations.PUT);
   }

   public void testReplace(Method method) {
      doTest(method, Operations.REPLACE);
   }

   public void testConditionalReplace(Method method) {
      doTest(method, Operations.REPLACE_IF);
   }

   public void testRemove(Method method) {
      doTest(method, Operations.REMOVE);
   }

   public void testConditionalRemove(Method method) {
      doTest(method, Operations.REMOVE_IF);
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
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
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

   private void doTest(Method method, TestConfig testConfig) {
      final String key = TestingUtil.k(method, 0);
      final String initialValue = testConfig.needsInitialValue() ? TestingUtil.v(method, 0) : null;

      //init cache if needed!
      if (testConfig.needsInitialValue()) {
         cache(siteName(0), 0).put(key, initialValue);
      }
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(initialValue, cache.get(key)));

      //disable xsite so each site won't send anything to the others
      iracManagerList.forEach(ManualIracManager::enable);

      //put a conflict value. each site has a different value for the same key
      String[] finalValues = new String[N_SITES];
      for (int i = 0; i < N_SITES; ++i) {
         String newValue = TestingUtil.v(method, (i + 1) * 2);
         if ((testConfig == Operations.REMOVE_IF || testConfig == Operations.REMOVE) && i > 0) {
            //to make sure remove works, we remove from LON only since it is the winning site.
            //the other sites put other value.
            cache(siteName(i), 0).put(key, newValue);
            finalValues[i] = newValue;
         } else {
            finalValues[i] = testConfig.perform(cache(siteName(i), 0), key, initialValue, newValue);
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


   private enum Operations implements TestConfig {
      PUT_IF_ABSENT {
         @Override
         public boolean needsInitialValue() {
            return false;
         }

         @Override
         public String perform(Cache<String, String> cache, String key, String prevValue, String newValue) {
            AssertJUnit.assertNull(prevValue);
            String prev = cache.putIfAbsent(key, newValue);
            AssertJUnit.assertNull(prev); //putIfAbsent must succeed.
            return newValue;
         }
      },
      PUT {
         @Override
         public boolean needsInitialValue() {
            return false;
         }

         @Override
         public String perform(Cache<String, String> cache, String key, String prevValue, String newValue) {
            AssertJUnit.assertNull(prevValue);
            cache.put(key, newValue);
            return newValue;
         }
      },
      REPLACE {
         @Override
         public boolean needsInitialValue() {
            return true;
         }

         @Override
         public String perform(Cache<String, String> cache, String key, String prevValue, String newValue) {
            AssertJUnit.assertNotNull(prevValue);
            cache.replace(key, newValue);
            return newValue;
         }
      },
      REPLACE_IF {
         @Override
         public boolean needsInitialValue() {
            return true;
         }

         @Override
         public String perform(Cache<String, String> cache, String key, String prevValue, String newValue) {
            AssertJUnit.assertNotNull(prevValue);
            cache.replace(key, prevValue, newValue);
            return newValue;
         }
      },
      REMOVE {
         @Override
         public boolean needsInitialValue() {
            return true;
         }

         @Override
         public String perform(Cache<String, String> cache, String key, String prevValue, String newValue) {
            AssertJUnit.assertNotNull(prevValue);
            cache.remove(key);
            return null;
         }
      },
      REMOVE_IF {
         @Override
         public boolean needsInitialValue() {
            return true;
         }

         @Override
         public String perform(Cache<String, String> cache, String key, String prevValue, String newValue) {
            AssertJUnit.assertNotNull(prevValue);
            cache.remove(key, prevValue);
            return null;
         }
      }
   }

   interface TestConfig {
      boolean needsInitialValue();

      String perform(Cache<String, String> cache, String key, String prevValue, String newValue);
   }
}
