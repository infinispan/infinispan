package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.filter.KeyFilter;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.TimeService;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationFunctionalTest")
public class ExpirationFunctionalTest extends SingleCacheManagerTest {

   protected final int SIZE = 10;
   protected ControlledTimeService timeService = new ControlledTimeService();
   protected StorageType storage;

   public Object[] factory() {
      return new Object[]{
            new ExpirationFunctionalTest().withStorage(StorageType.BINARY),
            new ExpirationFunctionalTest().withStorage(StorageType.OBJECT),
            new ExpirationFunctionalTest().withStorage(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected String parameters() {
      return "[" + storage + "]";
   }

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      configure(builder);
      EmbeddedCacheManager cm = createCacheManager(builder);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      cache = cm.getCache();
      afterCacheCreated(cm);
      return cm;
   }

   protected EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   protected void configure(ConfigurationBuilder config) {
      config
            .expiration().disableReaper()
            .memory().storageType(storage);
   }

   protected void afterCacheCreated(EmbeddedCacheManager cm) {

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
      assertEquals(0, cache.size());
   }

   public void testExpirationLifespanInOps() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      for (int i = 0; i < SIZE; i++) {
         assertFalse(cache.containsKey("key-" + 1));
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
         assertFalse(cache.containsKey("key-" + 1));
         assertNull(cache.get("key-" + i));
         assertNull(cache.remove("key-" + i));
      }
   }

   public void testExpirationLifespanInExec() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      for (int i = 0; i < SIZE; i++) {
         cache.getAdvancedCache().getDataContainer().executeTask(KeyFilter.ACCEPT_ALL_FILTER,
               (k, ice) -> { throw new RuntimeException("No task should be executed on expired entry"); });
      }
   }

   public void testExpirationMaxIdleInExec() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i,-1, null, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      for (int i = 0; i < SIZE; i++) {
         cache.getAdvancedCache().getDataContainer().executeTask(KeyFilter.ACCEPT_ALL_FILTER,
               (k, ice) -> { throw new RuntimeException("No task should be executed on expired entry"); });
      }
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
