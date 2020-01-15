package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationFunctionalTest")
public class ExpirationFunctionalTest extends SingleCacheManagerTest {

   protected final int SIZE = 10;
   protected ControlledTimeService timeService = new ControlledTimeService();
   protected StorageType storage;
   protected CacheMode cacheMode;
   protected ExpirationManager expirationManager;

   @Factory
   public Object[] factory() {
      return new Object[]{
         new ExpirationFunctionalTest().cacheMode(CacheMode.LOCAL).withStorage(StorageType.BINARY),
         new ExpirationFunctionalTest().cacheMode(CacheMode.LOCAL).withStorage(StorageType.OBJECT),
         new ExpirationFunctionalTest().cacheMode(CacheMode.LOCAL).withStorage(StorageType.OFF_HEAP),
         new ExpirationFunctionalTest().cacheMode(CacheMode.DIST_SYNC).withStorage(StorageType.BINARY),
         new ExpirationFunctionalTest().cacheMode(CacheMode.DIST_SYNC).withStorage(StorageType.OBJECT),
         new ExpirationFunctionalTest().cacheMode(CacheMode.DIST_SYNC).withStorage(StorageType.OFF_HEAP)
      };
   }

   protected ExpirationFunctionalTest cacheMode(CacheMode mode) {
      this.cacheMode = mode;
      return this;
   }

   @Override
   protected String parameters() {
      return "[" + cacheMode + ", " + storage + "]";
   }

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      configure(builder);
      EmbeddedCacheManager cm = createCacheManager(builder);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      cache = cm.getCache();
      expirationManager = cache.getAdvancedCache().getExpirationManager();
      afterCacheCreated(cm);
      return cm;
   }

   protected EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      if (builder.clustering().cacheMode().isClustered()) {
         return TestCacheManagerFactory.createClusteredCacheManager(builder);
      } else {
         return TestCacheManagerFactory.createCacheManager(builder);
      }
   }

   protected void configure(ConfigurationBuilder config) {
      config.clustering().cacheMode(cacheMode)
            .expiration().disableReaper()
            .memory().storageType(storage);
   }

   protected void afterCacheCreated(EmbeddedCacheManager cm) {

   }

   protected void processExpiration() {
      expirationManager.processExpiration();
   }

   public ExpirationFunctionalTest withStorage(StorageType storage) {
      this.storage = storage;
      return this;
   }

   public StorageType getStorageType() {
      return storage;
   }

   public void testSimpleExpirationLifespan() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);
      assertEquals(0, cache.size());
   }

   public void testSimpleExpirationMaxIdle() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i,-1, null, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      if (cacheMode.isClustered()) {
         assertEquals(SIZE, cache.size());
         processExpiration();
      }

      assertEquals(0, cache.size());
   }

   public void testSimpleExprationMaxIdleWithGet() {
      Object key = "max-idle-get-key";
      Object value = "max-idle-value";
      assertNull(cache.put(key, value,-1, null, 20, TimeUnit.MILLISECONDS));

      // Just before it expires
      timeService.advance(19);

      assertEquals(value, cache.get(key));

      timeService.advance(5);

      assertEquals(value, cache.get(key));

      timeService.advance(25);

      assertNull(cache.get(key));
   }

   public void testExpirationLifespanInOps() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      for (int i = 0; i < SIZE; i++) {
         assertFalse(cache.containsKey("key-" + i));
         assertNull(cache.get("key-" + i));
         assertNull(cache.remove("key-" + i));
      }
   }

   public void testExpirationMaxIdleInOps() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i,-1, null, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      for (int i = 0; i < SIZE; i++) {
         assertFalse(cache.containsKey("key-" + i));
         assertNull(cache.get("key-" + i));
         assertNull(cache.remove("key-" + i));
      }
   }

   public void testExpirationLifespanInExec() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      cache.getAdvancedCache().getDataContainer()
           .forEach(ice -> {
              throw new RuntimeException(
                 "No task should be executed on expired entry");
           });
   }

   public void testExpirationMaxIdleInExec() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i,-1, null, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      if (cacheMode.isClustered()) {
         AtomicInteger invocationCount = new AtomicInteger();
         cache.getAdvancedCache().getDataContainer().iterator().forEachRemaining(ice -> invocationCount.incrementAndGet());
         assertEquals(SIZE, invocationCount.get());
         processExpiration();
      }
      cache.getAdvancedCache().getDataContainer()
            .forEach(ice -> {
              throw new RuntimeException(
                 "No task should be executed on expired entry");
           });
   }

   public void testExpiredEntriesCleared() {
      cache.put("key-" + 0, "value-" + 1, -1, null, 0, TimeUnit.MILLISECONDS);
      cache.put("key-" + 1, "value-" + 1, -1, null, 1, TimeUnit.MILLISECONDS);

      // This should expire 1 of the entries
      timeService.advance(1);

      cache.clear();
      assertEquals(0, cache.getAdvancedCache().getDataContainer().sizeIncludingExpired());
   }
}
