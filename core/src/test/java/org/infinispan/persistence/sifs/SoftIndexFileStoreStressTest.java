package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestBlocking;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "persistence.sifs.SoftIndexFileStoreStressTest")
public class SoftIndexFileStoreStressTest extends SingleCacheManagerTest {
   private static final String CACHE_NAME = "stress-test-cache";
   protected String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(getClass());
   }

   @AfterClass(alwaysRun = true, dependsOnMethods = "destroyAfterClass")
   protected void clearTempDir() throws IOException {
      SoftIndexFileStoreTestUtils.StatsValue statsValue = SoftIndexFileStoreTestUtils.readStatsFile(tmpDirectory, CACHE_NAME, log);
      long dataDirectorySize = SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, CACHE_NAME);

      assertEquals(dataDirectorySize, statsValue.getStatsSize());
      Util.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory(this.getClass()));
      global.cacheContainer().security().authorization().enable();
      return TestCacheManagerFactory.newDefaultCacheManager(false, global, new ConfigurationBuilder());
   }

   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, String cacheName, boolean preload) {
      persistence
            .addSoftIndexFileStore()
            .dataLocation(Paths.get(tmpDirectory, "data").toString())
            .indexLocation(Paths.get(tmpDirectory, "index").toString())
            .maxFileSize(1000)
            .async().enabled(true)
            .purgeOnStartup(false).preload(preload)
            // Effectively disable reaper for tests
            .expiration().wakeUpInterval(Long.MAX_VALUE);
      return persistence;
   }

   public void testConstantReadsWithCompaction() throws InterruptedException, ExecutionException, TimeoutException {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), CACHE_NAME, false);
      TestingUtil.defineConfiguration(cacheManager, CACHE_NAME, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(CACHE_NAME);
      cache.start();

      int numKeys = 22;

      for (int i = 0; i < numKeys; ++i) {
         cache.put("key-" + i, "value-" + i);
      }

      WaitDelegatingNonBlockingStore<?, ?> store = TestingUtil.getFirstStoreWait(cache);

      Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

      AtomicBoolean continueRunning = new AtomicBoolean(true);
      Future<Void> retrievalFork = fork(() -> {
         while (continueRunning.get()) {
            int i = ThreadLocalRandom.current().nextInt(numKeys);
            assertEquals("value-" + i, cache.get("key-" + i));
         }
      });

      Future<Void> writeFork = fork(() -> {
         while (continueRunning.get()) {
            int i = ThreadLocalRandom.current().nextInt(numKeys);
            String sb = i +
                  "vjaofijeawofiejafioeh23uh123eu213heu1he u1ni 1uh13iueh 1iuehn12ujhen12ujhn2112w!@KEO@J!E I!@JEIO! J@@@E1j ie1jvjaofijeawofiejafioeha".repeat(i);
            cache.put("k" + i, sb);
         }
      });

      Future<Void> removeFork = fork(() -> {
         while (continueRunning.get()) {
            int i = ThreadLocalRandom.current().nextInt(numKeys);
            cache.remove("k" + i);
         }
      });

      Future<Void> compactionFork = fork(() -> {
         while (continueRunning.get()) {
            try {
               compactor.forceCompactionForAllNonLogFiles()
                     .toCompletableFuture().get(5, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
               throw e;
            }
         }
      });

      long startTime = System.nanoTime();
      long secondsToRun = TimeUnit.MINUTES.toSeconds(2);

      while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime) < secondsToRun) {
         if (retrievalFork.isDone() || compactionFork.isDone() || writeFork.isDone() || removeFork.isDone()) {
            continueRunning.set(false);
            break;
         }
         TestingUtil.sleepThread(200);
      }
      continueRunning.set(false);

      try {
         TestBlocking.get(retrievalFork, 10, TimeUnit.SECONDS);
         TestBlocking.get(compactionFork, 10, TimeUnit.SECONDS);
         TestBlocking.get(writeFork, 10, TimeUnit.SECONDS);
         TestBlocking.get(removeFork, 10, TimeUnit.SECONDS);
      } catch (Throwable t) {
         log.tracef(Util.threadDump());
         throw t;
      }
   }
}
