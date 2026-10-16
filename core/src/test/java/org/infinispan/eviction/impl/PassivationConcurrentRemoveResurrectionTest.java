package org.infinispan.eviction.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.impl.PassivationConcurrentRemoveResurrectionTest")
public class PassivationConcurrentRemoveResurrectionTest extends SingleCacheManagerTest {

   private static final int DATA_SIZE = 100;
   private static final int BATCH_SIZE = 10;
   private static final int MAX_ENTRIES = 1;

   public enum StoreType {
      DUMMY,
      SIFS
   }

   private StoreType storeType;
   private String tmpDirectory;

   public PassivationConcurrentRemoveResurrectionTest withStore(StoreType storeType) {
      this.storeType = storeType;
      return this;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new PassivationConcurrentRemoveResurrectionTest().withStore(StoreType.DUMMY),
            new PassivationConcurrentRemoveResurrectionTest().withStore(StoreType.SIFS)
      };
   }

   @Override
   protected String parameters() {
      return "[" + storeType + "]";
   }

   @AfterClass(alwaysRun = true)
   protected void removeTmpDirectory() {
      if (tmpDirectory != null) {
         Util.recursiveFileRemove(tmpDirectory);
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = new ConfigurationBuilder();
      cfg.persistence().passivation(true);
      switch (storeType) {
         case DUMMY:
            cfg.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).purgeOnStartup(true);
            break;
         case SIFS:
            tmpDirectory = Testing.tmpDirectory(getClass().getSimpleName() + "-" + storeType);
            cfg.persistence().addStore(SoftIndexFileStoreConfigurationBuilder.class)
                  .purgeOnStartup(true)
                  .dataLocation(Paths.get(tmpDirectory, "data").toString())
                  .indexLocation(Paths.get(tmpDirectory, "index").toString());
            break;
      }
      cfg.memory().maxCount(MAX_ENTRIES);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testConcurrentRemoveDoesNotResurrectEntry() throws Exception {
      Cache<String, String> testCache = cacheManager.getCache();

      for (int offset = 0; offset < DATA_SIZE; offset += BATCH_SIZE) {
         Map<String, String> batch = new HashMap<>();
         for (int i = offset; i < offset + BATCH_SIZE; i++) {
            batch.put("key_" + i, "value_" + i);
         }
         testCache.putAll(batch);
      }

      assertThat(testCache.size()).isEqualTo(DATA_SIZE);

      for (int offset = 0; offset < DATA_SIZE; offset += BATCH_SIZE) {
         List<CompletableFuture<String>> futures = new ArrayList<>(BATCH_SIZE);
         for (int i = offset; i < offset + BATCH_SIZE; i++) {
            futures.add(testCache.removeAsync("key_" + i));
         }
         CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
      }

      Map<String, String> survivors = new HashMap<>();
      for (int i = 0; i < DATA_SIZE; i++) {
         String key = "key_" + i;
         String value = testCache.get(key);
         if (value != null) {
            survivors.put(key, value);
         }
      }

      assertThat(survivors).as("removed entries resurrected from the store").isEmpty();
   }
}
