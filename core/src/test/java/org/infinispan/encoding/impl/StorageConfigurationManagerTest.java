package org.infinispan.encoding.impl;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "encoding.impl.StorageConfigurationManagerTest")
public class StorageConfigurationManagerTest extends SingleCacheManagerTest {

   public static final String CACHE_NAME = "testCache";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager();
   }

   public static long wallClockTime() {
      return TIME_SERVICE.wallClockTime();
   }

   public static long time() {
      return TIME_SERVICE.time();
   }

   public static Instant instant() {
      return TIME_SERVICE.instant();
   }

   public static long timeDuration(long startTimeNanos, TimeUnit outputTimeUnit) {
      return TIME_SERVICE.timeDuration(startTimeNanos, outputTimeUnit);
   }

   public static long timeDuration(long startTimeNanos, long endTimeNanos, TimeUnit outputTimeUnit) {
      return TIME_SERVICE.timeDuration(startTimeNanos, endTimeNanos, outputTimeUnit);
   }

   public static boolean isTimeExpired(long endTimeNanos) {
      return TIME_SERVICE.isTimeExpired(endTimeNanos);
   }

   public static long remainingTime(long endTimeNanos, TimeUnit outputTimeUnit) {
      return TIME_SERVICE.remainingTime(endTimeNanos, outputTimeUnit);
   }

   public static long expectedEndTime(long duration, TimeUnit inputTimeUnit) {
      return TIME_SERVICE.expectedEndTime(duration, inputTimeUnit);
   }

   public void testDefaultMediaType() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      assertStorageMediaTypes(configurationBuilder, StorageType.HEAP, StorageType.HEAP,
                              MediaType.APPLICATION_OBJECT);


      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().storage(StorageType.HEAP);
      assertStorageMediaTypes(configurationBuilder, StorageType.HEAP, StorageType.HEAP,
                              MediaType.APPLICATION_OBJECT);

      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().storage(StorageType.HEAP);
      assertStorageMediaTypes(configurationBuilder, StorageType.HEAP, StorageType.HEAP,
                              MediaType.APPLICATION_OBJECT);


      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().storage(StorageType.HEAP);
      assertStorageMediaTypes(configurationBuilder, StorageType.HEAP, StorageType.HEAP,
                              MediaType.APPLICATION_OBJECT);


      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().storage(StorageType.HEAP);
      assertStorageMediaTypes(configurationBuilder, StorageType.HEAP, StorageType.HEAP,
                              MediaType.APPLICATION_OBJECT);

      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().storage(StorageType.OFF_HEAP);
      assertStorageMediaTypes(configurationBuilder, StorageType.OFF_HEAP, StorageType.OFF_HEAP,
                              MediaType.APPLICATION_PROTOSTREAM);

      configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.memory().storage(StorageType.OFF_HEAP);
      assertStorageMediaTypes(configurationBuilder, StorageType.OFF_HEAP, StorageType.OFF_HEAP,
                              MediaType.APPLICATION_PROTOSTREAM);
   }

   private void assertStorageMediaTypes(ConfigurationBuilder configurationBuilder, StorageType storage,
                                        StorageType storageType, MediaType mediaType) {
      cacheManager.defineConfiguration(CACHE_NAME, configurationBuilder.build());
      Cache<Object, Object> cache = cacheManager.getCache(CACHE_NAME);
      Configuration cacheConfiguration = cache.getCacheConfiguration();
      assertEquals("Wrong storage", storage, cacheConfiguration.memory().storage());
      assertEquals("Wrong storageType", storageType, cacheConfiguration.memory().storage());
      assertEquals("Wrong heapConfiguration.storageType", storageType,
                   cacheConfiguration.memory().storage());
      StorageConfigurationManager scm = extractComponent(cache, StorageConfigurationManager.class);
      assertEquals("Wrong key media type", mediaType, scm.getKeyStorageMediaType());
      assertEquals("Wrong value media type", mediaType, scm.getValueStorageMediaType());
      cacheManager.administration().removeCache(CACHE_NAME);
   }
}
