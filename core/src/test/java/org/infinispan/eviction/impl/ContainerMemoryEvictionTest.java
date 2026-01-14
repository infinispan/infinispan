package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Arrays;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.impl.ContainerMemoryEvictionTest")
public class ContainerMemoryEvictionTest extends MultipleCacheManagersTest {
   enum RunMode {
      NO_PERSISTENCE,
      PERSISTENCE {
         @Override
         void updateConfig(ConfigurationBuilder config) {
            config.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
         }
      },
      PASSIVATION {
         @Override
         void updateConfig(ConfigurationBuilder config) {
            config.persistence().persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
         }
      };

      void updateConfig(ConfigurationBuilder config) { }
   }

   private RunMode runMode;

   public ContainerMemoryEvictionTest withRunMode(RunMode runMode) {
      this.runMode = runMode;
      return this;
   }

   private static final int NODE_COUNT = 3;

   private static final String SIZE_CONTAINER_NAME = "size-container";
   private static final String BYTE_CONTAINER_NAME = "byte-container";
   private static final String SIZE_DIST_CACHE_NAME = "size-dist-cache";
   private static final String BYTE_DIST_CACHE_NAME = "byte-dist-cache";
   private static final String SIZE_LOCAL_CACHE_NAME = "size-local-cache";
   private static final String BYTE_LOCAL_CACHE_NAME = "byte-local-cache";
   private static final int SIZE_CONTAINER_AMT = 10;
   private static final String BYTE_CONTAINER_AMT = "4096B";

   @Override
   public Object[] factory() {
      return Arrays.stream(RunMode.values())
                  .map(rm -> new ContainerMemoryEvictionTest().withRunMode(rm))
                  .toArray();
   }

   @Override
   protected Object[] parameterValues() {
      return new Object[] { runMode };
   }

   @Override
   protected String[] parameterNames() {
      return new String[] { "runMode" };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NODE_COUNT; i++) {
         GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
         gcb.containerMemoryConfiguration(SIZE_CONTAINER_NAME).maxCount(SIZE_CONTAINER_AMT)
               .containerMemoryConfiguration(BYTE_CONTAINER_NAME).maxSize(BYTE_CONTAINER_AMT);

         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(gcb, new ConfigurationBuilder());
         cacheManagers.add(cm);
      }

      ConfigurationBuilder distCacheBuilder = new ConfigurationBuilder();
      runMode.updateConfig(distCacheBuilder);
      distCacheBuilder.clustering().cacheMode(CacheMode.DIST_SYNC)
            .memory().evictionContainer(SIZE_CONTAINER_NAME);

      ConfigurationBuilder localCacheBuilder = new ConfigurationBuilder();
      runMode.updateConfig(localCacheBuilder);
      localCacheBuilder.memory().evictionContainer(SIZE_CONTAINER_NAME);

      defineConfigurationOnAllManagers(SIZE_DIST_CACHE_NAME, distCacheBuilder);
      defineConfigurationOnAllManagers(SIZE_LOCAL_CACHE_NAME, localCacheBuilder);

      distCacheBuilder.memory().evictionContainer(BYTE_CONTAINER_NAME);
      localCacheBuilder.memory().evictionContainer(BYTE_CONTAINER_NAME);

      defineConfigurationOnAllManagers(BYTE_DIST_CACHE_NAME, distCacheBuilder);
      defineConfigurationOnAllManagers(BYTE_LOCAL_CACHE_NAME, localCacheBuilder);

      waitForClusterToForm(SIZE_DIST_CACHE_NAME, BYTE_DIST_CACHE_NAME);
   }

   private int distSize(String distCacheName) {
      int count = 0;
      for (int i = 0; i < NODE_COUNT; ++i) {
         count += cache(i, distCacheName).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_LOAD).size();
      }
      return count;
   }

   private void testContainerEviction(String distCacheName, String localCacheName) {
      Cache<Integer, Integer> distCache = cache(0, distCacheName);
      Cache<Integer, Integer> localCache = cache(0, localCacheName);

      int expectedSize = 0;
      int totalDistSize;
      // Insert until we have an eviction on at least one distributed cache
      do {
         expectedSize += 2;
         distCache.put(expectedSize, expectedSize);
         totalDistSize = distSize(distCacheName);
      } while (expectedSize == totalDistSize);

      // Insert some more just to be sure all 3 caches are full
      for (int i = 0; i < 10; ++i) {
         // Insert one more just to be safe
         distCache.put(++expectedSize, ++expectedSize);
      }

      // Retrieve the size of the distributed cache using the LOCAL_MODE flag
      long distSize = distCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_LOAD).size();

      // Now insert some into the first local cache which should cause some evictions
      for (int i = 1; i <= 5; i++) {
         localCache.put(expectedSize + i, expectedSize + i);
      }

      // Afterwards the dist size and local size should equal what the dist size before local insertion
      int afterDistSize = distCache.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL, Flag.SKIP_CACHE_LOAD).size();
      int localSize = localCache.size();
      if (afterDistSize > distSize) {
         fail("Size of local " + localSize + " and dist " + afterDistSize + " caches should be equal or less to prior dist size " + distSize);
      }

      // All persistence modes should have all data
      if (runMode != RunMode.NO_PERSISTENCE) {
         assertEquals(expectedSize / 2, distCache.size());
         assertEquals(5, localCache.size());
      }
   }

   public void testSizeEviction() {
      testContainerEviction(SIZE_DIST_CACHE_NAME, SIZE_LOCAL_CACHE_NAME);
   }

   public void testByteEviction() {
      testContainerEviction(BYTE_DIST_CACHE_NAME, BYTE_LOCAL_CACHE_NAME);
   }
}
