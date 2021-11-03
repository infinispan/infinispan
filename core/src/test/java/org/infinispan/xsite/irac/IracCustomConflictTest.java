package org.infinispan.xsite.irac;

import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.TestOperation;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.infinispan.xsite.spi.SiteEntry;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "xsite.irac.IracCustomConflictTest")
public class IracCustomConflictTest extends AbstractMultipleSitesTest {
   private static final int N_SITES = 2;
   private static final int CLUSTER_SIZE = 3;
   private final List<ManualIracManager> iracManagerList;

   private final ConfigMode site1Config;
   private final ConfigMode site2Config;

   IracCustomConflictTest(ConfigMode site1Config, ConfigMode site2Config) {
      this.site1Config = site1Config;
      this.site2Config = site2Config;
      this.iracManagerList = new ArrayList<>(N_SITES * CLUSTER_SIZE);
   }

   @Factory
   public static Object[] defaultFactory() {
      ConfigMode[] values = ConfigMode.values();
      Object[] tests = new Object[values.length * values.length];
      int i = 0;
      for (ConfigMode s1 : values) {
         for (ConfigMode s2 : values) {
            tests[i++] = new IracCustomConflictTest(s1, s2);
         }
      }
      return tests;
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
   protected String[] parameterNames() {
      return new String[]{null, null};
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[]{site1Config, site2Config};
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
      ConfigMode configMode = siteIndex == 0 ? site1Config : site2Config;
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      switch (configMode) {
         case P_TX:
            builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
                  .lockingMode(LockingMode.PESSIMISTIC);
            break;
         case O_TX:
            builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
                  .lockingMode(LockingMode.OPTIMISTIC);
            break;
         case NO_TX:
         default:
            //no-op
      }
      builder.sites().mergePolicy(new CustomEntryMergePolicy());
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
      MySortedSet initialValue;
      if (testConfig != TestOperation.PUT_IF_ABSENT) {
         initialValue = new MySortedSet(new String[]{"a"});
         cache(siteName(0), 0).put(key, initialValue);
         eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(initialValue, cache.get(key)));
      } else {
         initialValue = null;
      }

      //disable xsite so each site won't send anything to the others
      iracManagerList.forEach(ManualIracManager::enable);

      //put a conflict value. each site has a different value for the same key
      MySortedSet[] finalValues = new MySortedSet[N_SITES];
      for (int i = 0; i < N_SITES; ++i) {
         MySortedSet newValue = initialValue == null ?
                                new MySortedSet(new String[]{"a", "site_" + i}) : //putIfAbsent, add "a" element
                                initialValue.add("site_" + i);
         if (i == 0) {
            finalValues[i] = testConfig.execute(cache(siteName(i), 0), key, initialValue, newValue);
         } else {
            cache(siteName(i), 0).put(key, newValue);
            finalValues[i] = newValue;
         }
      }

      //check if everything is correct
      for (int i = 0; i < N_SITES; ++i) {
         MySortedSet fValue = finalValues[i];
         assertInSite(siteName(i), cache -> assertEquals(fValue, cache.get(key)));
      }

      //enable xsite. this will send the keys!
      iracManagerList.forEach(manualIracManager -> manualIracManager.disable(ManualIracManager.DisableMode.SEND));

      MySortedSet finalValue = testConfig == TestOperation.REMOVE || testConfig == TestOperation.REMOVE_CONDITIONAL ?
                               new MySortedSet(new String[]{"a", "site_1"}) :
                               new MySortedSet(new String[]{"a", "site_0", "site_1"}); //the values should be merged.
      eventuallyAssertInAllSitesAndCaches(cache -> Objects.equals(finalValue, cache.get(key)));
      assertNoDataLeak(null);
   }

   private enum ConfigMode {
      NO_TX,
      P_TX,
      O_TX
   }

   public static class CustomEntryMergePolicy implements XSiteEntryMergePolicy<String, MySortedSet> {

      @Override
      public CompletionStage<SiteEntry<MySortedSet>> merge(String key, SiteEntry<MySortedSet> localEntry,
                                                           SiteEntry<MySortedSet> remoteEntry) {
         MySortedSet local = localEntry.getValue();
         MySortedSet remote = remoteEntry.getValue();
         if (local == remote) {
            return CompletableFuture.completedFuture(compare(localEntry, remoteEntry) < 0 ? localEntry : remoteEntry);
         } else if (local == null) {
            return CompletableFuture.completedFuture(remoteEntry);
         } else if (remote == null) {
            return CompletableFuture.completedFuture(localEntry);
         }
         //both are not null
         MySortedSet result = local.addAll(remote);
         String site = compare(localEntry, remoteEntry) < 0 ? localEntry.getSiteName() : remoteEntry.getSiteName();
         return CompletableFuture.completedFuture(new SiteEntry<>(site, result, null));
      }

      private int compare(SiteEntry<MySortedSet> local, SiteEntry<MySortedSet> remote) {
         return local.getSiteName().compareTo(remote.getSiteName());
      }
   }

   public static class MySortedSet {
      private final String[] data;

      @ProtoFactory
      public MySortedSet(String[] data) {
         this.data = Objects.requireNonNull(data);
      }

      public boolean contains(String element) {
         return Arrays.binarySearch(data, element) >= 0;
      }

      public MySortedSet add(String element) {
         if (contains(element)) {
            return this;
         }
         String[] newData = Arrays.copyOf(data, data.length + 1);
         newData[data.length] = element;
         Arrays.sort(newData);
         return new MySortedSet(newData);
      }

      public MySortedSet addAll(MySortedSet other) {
         List<String> newElements = new LinkedList<>();
         for (String e : other.data) {
            if (contains(e)) {
               continue;
            }
            newElements.add(e);
         }
         if (newElements.isEmpty()) {
            return this;
         }
         String[] newData = Arrays.copyOf(data, data.length + newElements.size());
         int i = data.length;
         for (String e : newElements) {
            newData[i++] = e;
         }
         Arrays.sort(newData);
         return new MySortedSet(newData);
      }

      @ProtoField(number = 1)
      public String[] getData() {
         return data;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }

         MySortedSet that = (MySortedSet) o;

         // Probably incorrect - comparing Object[] arrays with Arrays.equals
         return Arrays.equals(data, that.data);
      }

      @Override
      public int hashCode() {
         return Arrays.hashCode(data);
      }
   }
}
