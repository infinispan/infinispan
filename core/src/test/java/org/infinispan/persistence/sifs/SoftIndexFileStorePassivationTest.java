package org.infinispan.persistence.sifs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestBlocking;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.sifs.SoftIndexFileStorePassivationTest")
public class SoftIndexFileStorePassivationTest extends SingleCacheManagerTest {
   private static final String CACHE_NAME = "passivation-test";
   private static final int ENTRIES = 3000;
   private static final int VALUE_BYTES = 256 * 1024;
   private static final int MAX_COUNT = 100;
   private static final int ROUNDS = 20;

   private final String tmpDirectory = Testing.tmpDirectory(getClass());

   @AfterClass(alwaysRun = true, dependsOnMethods = "destroyAfterClass")
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(tmpDirectory);
      global.globalState().enable();
      return TestCacheManagerFactory.newDefaultCacheManager(true, global, new ConfigurationBuilder());
   }

   public void testNoDataLossDuringPassivationAndCompaction() throws Throwable {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      cb.memory().maxCount(MAX_COUNT);
      cb.expiration().lifespan(3600000).wakeUpInterval(1000);
      cb.persistence().passivation(true)
            .addSoftIndexFileStore()
            .purgeOnStartup(true);

      cacheManager.defineConfiguration(CACHE_NAME, cb.build());
      Cache<String, byte[]> cache = cacheManager.getCache(CACHE_NAME);

      for (int i = 0; i < ENTRIES; i++) {
         cache.put("k" + i, new byte[VALUE_BYTES]);
      }

      AtomicBoolean stop = new AtomicBoolean(false);
      Future<?> churnFuture = fork(() -> {
         for (long i = 0; !stop.get(); i++) {
            cache.get("k" + ((i * 1009) % ENTRIES));
         }
      });

      try {
         for (int round = 0; round < ROUNDS; round++) {
            for (int i = 0; i < ENTRIES; i++) {
               String key = "k" + i;
               assertNotNull(cache.get(key), "Entry for " + key + " was lost in round " + round);
            }
         }
      } finally {
         stop.set(true);
         TestBlocking.get(churnFuture, 30, TimeUnit.SECONDS);
      }
   }
}
