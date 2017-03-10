package org.infinispan.container;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "stress", testName = "container.InvocationExpirationStressTest")
public class InvocationExpirationStressTest extends SingleCacheManagerTest {
   private volatile boolean terminated;
   private ControlledTimeService timeService = new ControlledTimeService();
   private ExecutorService executorService = Executors.newFixedThreadPool(3);
   private ExpirationManager expirationManager;
   private InvocationManager invocationManager;
   private DataContainer dataContainer;
   private long invocationTimeout;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.expiration().reaperEnabled(false);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
      cm.defineConfiguration("test", builder.build());
      cache = cm.getCache("test").getAdvancedCache().withFlags(Flag.SKIP_LOCKING);
      expirationManager = TestingUtil.extractComponent(cache, ExpirationManager.class);
      invocationManager = TestingUtil.extractComponent(cache, InvocationManager.class);
      invocationTimeout = invocationManager.invocationTimeout();
      dataContainer = TestingUtil.extractComponent(cache, DataContainer.class);
      TestingUtil.replaceComponent(cache, TimeService.class, timeService, true);
      return cm;
   }

   @AfterMethod(alwaysRun = true)
   public void cleanup() {
      executorService.shutdownNow();
   }

   public void testExpiration() throws Exception {
      cache.put("k", "v0");
      CompletableFuture<?> expirationFuture = CompletableFuture.runAsync(() -> {
         while (!terminated) {
            expirationManager.processExpiration();
            checkDataContainer();
         }
      }, executorService);
      CompletableFuture<?> updateFuture = CompletableFuture.runAsync(() -> {
         for (int i = 1; !terminated; ++i) {
            assertEquals("v" + (i - 1), cache.remove("k"));
            timeService.advance(1000 + ThreadLocalRandom.current().nextInt(1000));
            assertEquals(null, cache.put("k", "v" + i));
            timeService.advance(1000 + ThreadLocalRandom.current().nextInt(1000));
            checkDataContainer();
         }
      }, executorService);
      CompletableFuture.anyOf(expirationFuture, updateFuture).get(30, TimeUnit.SECONDS);
      terminated = true;
   }

   private void checkDataContainer() {
      Iterator<InternalCacheEntry> it = dataContainer.iteratorIncludingExpiredAndTombstones();
      assertTrue(it.hasNext());
      InternalCacheEntry entry = it.next();
      assertEquals("k", entry.getKey());
      assertNotNull(entry.getMetadata());
      InvocationRecord records = entry.getMetadata().lastInvocation();
      assertNotNull(records);
      int numRecords = records.numRecords();
      assertTrue("Records (" + numRecords + "): " + records, numRecords >= 1 && numRecords <= invocationTimeout / 1000);
      assertFalse(it.hasNext());
   }
}
