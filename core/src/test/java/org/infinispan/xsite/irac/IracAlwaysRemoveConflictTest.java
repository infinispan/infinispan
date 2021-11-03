package org.infinispan.xsite.irac;

import static org.testng.AssertJUnit.assertEquals;

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
import org.infinispan.util.TestOperation;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.spi.AlwaysRemoveXSiteEntryMergePolicy;
import org.infinispan.xsite.spi.XSiteMergePolicy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Function test for {@link AlwaysRemoveXSiteEntryMergePolicy}.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "xsite.irac.IracAlwaysRemoveConflictTest")
public class IracAlwaysRemoveConflictTest extends AbstractMultipleSitesTest {
   private static final int N_SITES = 2;
   private static final int CLUSTER_SIZE = 3;
   private final List<ManualIracManager> iracManagerList;

   protected IracAlwaysRemoveConflictTest() {
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
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.sites().mergePolicy(XSiteMergePolicy.ALWAYS_REMOVE);
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
         assertInSite(siteName(i), cache -> assertEquals(fValue, cache.get(key)));
      }

      //enable xsite. this will send the keys!
      iracManagerList.forEach(manualIracManager -> manualIracManager.disable(ManualIracManager.DisableMode.SEND));

      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(null, cache.get(key)));
      assertNoDataLeak(null);
   }
}
