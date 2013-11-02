package org.infinispan.persistence.file;

import java.io.File;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.junit.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.file.SingleFileStoreConcurrentTest")
public class SingleFileStoreConcurrentTest extends AbstractInfinispanTest {
   private static final long DURATION_SECONDS = 20;
   private static final long SLEEP_MILLISECONDS = 100;

   private String location;

   @BeforeClass
   protected void setUpTempDir() {
      this.location = TestingUtil.tmpDirectory(this);
   }

   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(this.location);
      new File(this.location).mkdirs();
   }

   public void test() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .locking().isolationLevel(IsolationLevel.REPEATABLE_READ)
         .transaction().lockingMode(LockingMode.PESSIMISTIC)
         .expiration().reaperEnabled(false)
         .eviction().strategy(EvictionStrategy.NONE)
         .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
               .transactionManagerLookup(new DummyTransactionManagerLookup())
         .invocationBatching().enable()
         .persistence().passivation(true).addSingleFileStore().location(this.location)
      ;
      EmbeddedCacheManager container = TestCacheManagerFactory.createCacheManager(builder);
      try {
         final Cache<String, Integer> cache = container.getCache();
         final String key1 = "a";
         final String key2 = "b";
         cache.startBatch();
         cache.put(key1, 1);
         cache.put(key2, 2);
         cache.endBatch(true);
         for (int i = 0; i < 1000; ++i) {
            cache.startBatch();
            cache.evict(key1);
            cache.endBatch(true);
            cache.startBatch();
            Assert.assertNotNull(cache.get(key1));
            cache.endBatch(true);
         }
         Runnable getTask = new Runnable() {
            @Override
            public void run() {
               try {
                  while (!Thread.currentThread().isInterrupted()) {
                     cache.startBatch();
                     try {
                        Object value = cache.get(key1);
                        Assert.assertNotNull(value);
                        value = cache.get(key2);
                        Assert.assertNotNull(value);
                     } finally {
                        cache.endBatch(true);
                     }
                     TimeUnit.MILLISECONDS.sleep(SLEEP_MILLISECONDS);
                  }
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
            }
         };
         Runnable evictTask1 = new Runnable() {
            @Override
            public void run() {
               try {
                  while (!Thread.currentThread().isInterrupted()) {
                     cache.startBatch();
                     try {
                        if (cache.getAdvancedCache().withFlags(Flag.FAIL_SILENTLY).lock(key1)) {
                           cache.getAdvancedCache().withFlags(Flag.SKIP_LOCKING).evict(key1);
                        }
                     } finally {
                        cache.endBatch(true);
                     }
                     TimeUnit.MILLISECONDS.sleep(SLEEP_MILLISECONDS);
                  }
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
            }
         };
         Runnable evictTask2 = new Runnable() {
            @Override
            public void run() {
               try {
                  while (!Thread.currentThread().isInterrupted()) {
                     cache.startBatch();
                     try {
                        if (cache.getAdvancedCache().withFlags(Flag.FAIL_SILENTLY).lock(key2)) {
                           cache.getAdvancedCache().withFlags(Flag.SKIP_LOCKING).evict(key2);
                        }
                     } finally {
                        cache.endBatch(true);
                     }
                     TimeUnit.MILLISECONDS.sleep(SLEEP_MILLISECONDS);
                  }
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
            }
         };
         ExecutorService executor = Executors.newFixedThreadPool(3);
         try {
            Future<?> getFuture = executor.submit(getTask);
            Future<?> evictFuture1 = executor.submit(evictTask1);
            Future<?> evictFuture2 = executor.submit(evictTask2);
            try {
               TimeUnit.SECONDS.sleep(DURATION_SECONDS);
               getFuture.cancel(true);
               evictFuture1.cancel(true);
               evictFuture2.cancel(true);
               try {
                  getFuture.get();
                  evictFuture1.get();
                  evictFuture2.get();
               } catch (CancellationException e) {
                  // Allow
               } catch (ExecutionException e) {
                  e.getCause().printStackTrace();
                  Assert.fail(e.getMessage());
               }
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
            }
         } finally {
            executor.shutdownNow();
         }
      } finally {
         TestingUtil.killCacheManagers(container);
      }
   }
}
