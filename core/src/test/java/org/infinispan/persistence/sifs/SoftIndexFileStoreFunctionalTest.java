package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.sifs.SoftIndexFileStoreFunctionalTest")
public class SoftIndexFileStoreFunctionalTest extends BaseStoreFunctionalTest {
   protected String tmpDirectory;

   protected int segmentCount;
   protected boolean segmented;

   public SoftIndexFileStoreFunctionalTest(int segmentCount, boolean segmented) {
      this.segmentCount = segmentCount;
      this.segmented = segmented;
   }

   @Factory
   public static Object[] factory() {
      return new Object[] {
            new SoftIndexFileStoreFunctionalTest(1, true),
            // This should be effectively the same as 1 segment with segmented
            new SoftIndexFileStoreFunctionalTest(256, false),
            new SoftIndexFileStoreFunctionalTest(10, true),
            new SoftIndexFileStoreFunctionalTest(256, true),
            new SoftIndexFileStoreFunctionalTest(2048, true),
      };
   }

   @Override
   protected String parameters() {
      return "[" + segmentCount + ", " + segmented + "]";
   }

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected ConfigurationBuilder getDefaultCacheConfiguration() {
      ConfigurationBuilder configurationBuilder = super.getDefaultCacheConfiguration();
      configurationBuilder.clustering().hash().numSegments(segmentCount);
      return configurationBuilder;
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload) {
      persistence
            .addSoftIndexFileStore()
            .segmented(segmented)
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .purgeOnStartup(false).preload(preload)
            // Effectively disable reaper for tests
            .expiration().wakeUpInterval(Long.MAX_VALUE);
      return persistence;
   }

   public void testWritingSameKeyShortTimes() {
      String cacheName = "testWritingSameKeyShortTimes";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);

      for (int i = 0; i < Short.MAX_VALUE + 1; ++i) {
         cache.put("k", "v");
      }

      cache.remove("k");
   }

   @DataProvider(name = "keyArgs")
   public Object[][] keyConfiguration() {
      return Stream.of(10, 10_000, 250_000)
            .flatMap(keyCount ->
                  Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .map(largeKey -> new Object[] {
                              keyCount, largeKey
                        })
            ).toArray(Object[][]::new);
   }

   @Test(dataProvider = "keyArgs")
   public void testWriteManyDifferentKeysAndIterate(int keyCount, boolean largeKey) {
      String cacheName = "testWriteManyDifferentKeysAndIterate";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);

      for (int i = 0; i < keyCount; ++i) {
         int anotherValue = i * 13 + (i-1) * 19;
         String key = "k" + i + (largeKey ? "-" + anotherValue : "");
         cache.put(key, "v" + i + "-" + anotherValue);
      }

      // Force to read from store
      cache.getAdvancedCache().getDataContainer().clear();

      var list = new ArrayList<>(cache.entrySet());
      if (list.size() > keyCount) {
         Set<String>  duplicateKeys = new HashSet<>();
         var dupList = list.stream().map(Map.Entry::getKey)
               .filter(k -> !duplicateKeys.add(k))
               .map(k -> Map.entry(k, TestingUtil.extractComponent(cache, KeyPartitioner.class).getSegment(k)))
               .collect(Collectors.toList());
         fail("List contained a duplicate element" + dupList);
      } else {
         assertEquals(keyCount, list.size());
      }
   }
}
