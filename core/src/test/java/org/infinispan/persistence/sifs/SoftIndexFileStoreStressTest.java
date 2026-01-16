package org.infinispan.persistence.sifs;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.Cache;
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
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.internal.subscriptions.AsyncSubscription;
import io.reactivex.rxjava3.subscribers.TestSubscriber;

@Test(groups = "stress", testName = "persistence.sifs.SoftIndexFileStoreStressTest")
public class SoftIndexFileStoreStressTest extends SingleCacheManagerTest {
   private static final String CACHE_NAME = "stress-test-cache";
   protected String tmpDirectory;

   @BeforeClass(alwaysRun = true)
   protected void setUpTempDir() {
      tmpDirectory = Testing.tmpDirectory(getClass());
   }

   @AfterClass(alwaysRun = true, dependsOnMethods = "destroyAfterClass")
   protected void clearTempDir() throws IOException {
      try {
         SoftIndexFileStoreTestUtils.StatsValue statsValue = SoftIndexFileStoreTestUtils.readStatsFile(tmpDirectory, CACHE_NAME, log);
         long dataDirectorySize = SoftIndexFileStoreTestUtils.dataDirectorySize(tmpDirectory, CACHE_NAME);

         assertEquals(dataDirectorySize, statsValue.getStatsSize());
      } finally {
         Util.recursiveFileRemove(tmpDirectory);
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(Testing.tmpDirectory(this.getClass()));
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

   private enum Operation {
      READ {
         @Override
         public void execute(Cache<String, Object> cache, int keySpace) {
            int i = ThreadLocalRandom.current().nextInt(keySpace);
            Object value = cache.get("key-" + i);
            if (value != null) {
               assertEquals("value-" + i, value);
            }
         }
      },

      WRITE {
         @Override
         public void execute(Cache<String, Object> cache, int keySpace) {
            int i = ThreadLocalRandom.current().nextInt(keySpace);
            String sb = i +
                  "vjaofijeawofiejafioeh23uh123eu213heu1he u1ni 1uh13iueh 1iuehn12ujhen12ujhn2112w!@KEO@J!E I!@JEIO! J@@@E1j ie1jvjaofijeawofiejafioeha".repeat(i);
            cache.put("k" + i, sb);
         }
      },

      REMOVE {
         @Override
         public void execute(Cache<String, Object> cache, int keySpace) {
            int i = ThreadLocalRandom.current().nextInt(keySpace);
            cache.remove("k" + i);
         }
      },

      CLEAR {
         @Override
         public void execute(Cache<String, Object> cache, int keySpace) {
            cache.clear();
            try {
               Thread.sleep(20);
            } catch (InterruptedException ignore) {}
         }
      },

      ITERATE {
         @Override
         public void execute(Cache<String, Object> cache, int keySpace) {
            var list = new ArrayList<>(cache.entrySet());
            if (list.size() == 1000) {
               log.tracef("List size was: " + list.size());
            }
         }
      },


      COMPACTION {
         @Override
         public void execute(Cache<String, Object> cache, int keySpace) {
            WaitDelegatingNonBlockingStore<?, ?> store = TestingUtil.getFirstStoreWait(cache);
            Compactor compactor = TestingUtil.extractField(store.delegate(), "compactor");

            TestSubscriber<Object> testSubscriber = TestSubscriber.create();
            testSubscriber.onSubscribe(new AsyncSubscription());
            Compactor.CompactionExpirationSubscriber expSub = new Compactor.CompactionExpirationSubscriber() {
               @Override
               public void onEntryPosition(EntryPosition entryPosition) { }

               @Override
               public void onEntryEntryRecord(EntryRecord entryRecord) { }

               @Override
               public void onComplete() {
                  testSubscriber.onComplete();
               }

               @Override
               public void onError(Throwable t) {
                  testSubscriber.onError(t);
               }
            };

            compactor.performExpirationCompaction(expSub);
            testSubscriber.awaitDone(5, TimeUnit.SECONDS)
                  .assertComplete().assertNoErrors();
         }
      };

      public abstract void execute(Cache<String, Object> cache, int keySpace);
   }

   public void testConstantReadsWithCompaction() throws Throwable {
      ConfigurationBuilder cb = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      createCacheStoreConfig(cb.persistence(), CACHE_NAME, false);
      TestingUtil.defineConfiguration(cacheManager, CACHE_NAME, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(CACHE_NAME);
      cache.start();

      AtomicBoolean continueRunning = new AtomicBoolean(true);
      List<Future<?>> futures = new ArrayList<>();
      for (Operation operation : Operation.values()) {
         futures.add(fork(() -> runOperationOnCache(cache, continueRunning, operation)));
      }

      long startTime = System.nanoTime();
      long secondsToRun = TimeUnit.MINUTES.toSeconds(10);

      while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime) < secondsToRun) {
         if (futures.stream().anyMatch(Future::isDone)) {
            continueRunning.set(false);
            break;
         }
         TestingUtil.sleepThread(200);
      }
      continueRunning.set(false);

      try {
         get(10, TimeUnit.SECONDS, futures.toArray(Future[]::new));
      } catch (Throwable t) {
         log.tracef(Util.threadDump());
         throw t;
      }

      // Kill and restart the cache just to make sure we can use data
      TestingUtil.killCacheManagers(cacheManager);
      cacheManager = createCacheManager();

      TestingUtil.defineConfiguration(cacheManager, CACHE_NAME, cb.build());

      Cache newCache = cacheManager.getCache(CACHE_NAME);
      newCache.start();

      List<Object> entries = new ArrayList<>(newCache.entrySet());
      log.info("Size of entries after restart was: " + entries.size());
   }

   private void runOperationOnCache(Cache<String, Object> cache, AtomicBoolean continueRunning, Operation operation) {
      int numKeys = 22;
      for (int i = 0; i < numKeys; ++i) {
         cache.put("key-" + i, "value-" + i);
      }

      while (continueRunning.get()) {
         operation.execute(cache, numKeys);
      }
      cache.clear();
   }

   private void get(long time, TimeUnit unit, Future<?> ... futures) throws Throwable {
      for (Future<?> future : futures) {
         TestBlocking.get(future, time, unit);
      }
   }
}
