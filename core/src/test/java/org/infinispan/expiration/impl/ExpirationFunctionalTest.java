package org.infinispan.expiration.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.expiration.ExpirationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "expiration.impl.ExpirationFunctionalTest")
public class ExpirationFunctionalTest extends SingleCacheManagerTest {

   protected final int SIZE = 10;
   protected ControlledTimeService timeService = new ControlledTimeService();
   protected StorageType storage;
   protected CacheMode cacheMode;
   protected ExpirationManager<?, ?> expirationManager;

   @Factory
   public Object[] factory() {
      return new Object[]{
         new ExpirationFunctionalTest().cacheMode(CacheMode.LOCAL).withStorage(StorageType.BINARY),
         new ExpirationFunctionalTest().cacheMode(CacheMode.LOCAL).withStorage(StorageType.HEAP),
         new ExpirationFunctionalTest().cacheMode(CacheMode.LOCAL).withStorage(StorageType.OFF_HEAP),
         new ExpirationFunctionalTest().cacheMode(CacheMode.DIST_SYNC).withStorage(StorageType.BINARY),
         new ExpirationFunctionalTest().cacheMode(CacheMode.DIST_SYNC).withStorage(StorageType.HEAP),
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
      // Create the cache manager, but don't start it until we replace the time service
      EmbeddedCacheManager cm = createCacheManager(builder);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService, true);
      cm.start();
      cache = cm.getCache();
      expirationManager = cache.getAdvancedCache().getExpirationManager();
      afterCacheCreated(cm);
      return cm;
   }

   protected EmbeddedCacheManager createCacheManager(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder globalBuilder;
      if (builder.clustering().cacheMode().isClustered()) {
         globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
         configure(globalBuilder);
         return TestCacheManagerFactory.createClusteredCacheManager(false, globalBuilder, builder, new TransportFlags());
      } else {
         globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
         configure(globalBuilder);
         return TestCacheManagerFactory.createCacheManager(globalBuilder, builder, false);
      }
   }

   protected void configure(GlobalConfigurationBuilder globalBuilder) {
      globalBuilder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
   }

   protected void configure(ConfigurationBuilder config) {
      config.clustering().cacheMode(cacheMode)
            .expiration().disableReaper()
            .memory().storage(storage);
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
      assertEquals(0, cache.getAdvancedCache().withFlags(Flag.SKIP_SIZE_OPTIMIZATION).size());
   }

   protected int maxInMemory() {
      return SIZE;
   }

   public void testSimpleExpirationMaxIdle() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i, -1, null, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      assertEquals(0, cache.size());

      // Only processExpiration actually removes the entries
      assertEquals(maxInMemory(), cache.getAdvancedCache().getDataContainer().sizeIncludingExpired());
      processExpiration();
      assertEquals(0, cache.size());
      assertEquals(0, cache.getAdvancedCache().getDataContainer().sizeIncludingExpired());
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
         long expirationTime = i % 2 == 0 ? 1 : 1000;
         cache.put("key-" + i, "value-" + i, expirationTime, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      checkOddExist(SIZE);
   }

   public void testExpirationMaxIdleInOps() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         long expirationTime = i % 2 == 0 ? 1 : 1000;
         cache.put("key-" + i, "value-" + i, -1, null, expirationTime, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      checkOddExist(SIZE);
   }

   protected Object keyToUseWithExpiration() {
      return "key";
   }

   public void testRemoveExpiredValueWithNoEquals() {
      Object keyToUseForExpiration = keyToUseWithExpiration();
      cache.put(keyToUseForExpiration, new NoEquals("value"), 3, TimeUnit.MILLISECONDS);

      timeService.advance(5);

      expirationManager.processExpiration();

      assertEquals(0, cache.getAdvancedCache().getDataContainer().sizeIncludingExpired());
   }

   protected void checkOddExist(int SIZE) {
      for (int i = 0; i < SIZE; i++) {
         if (i % 2 == 0) {
            assertFalse(cache.containsKey("key-" + i));
            assertNull(cache.get("key-" + i));
            assertNull(cache.remove("key-" + i));
         } else {
            assertTrue(cache.containsKey("key-" + i));
            assertNotNull(cache.get("key-" + i));
            assertNotNull(cache.remove("key-" + i));
         }
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

   public void testExpirationMaxIdleDataContainerIterator() throws Exception {
      for (int i = 0; i < SIZE; i++) {
         cache.put("key-" + i, "value-" + i,-1, null, 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(2);

      cache.getAdvancedCache().getDataContainer()
            .iterator().forEachRemaining(ice -> {
              throw new RuntimeException("No task should be executed on expired entry");
           });
      cache.getAdvancedCache().getDataContainer()
           .forEach(ice -> {
              throw new RuntimeException("No task should be executed on expired entry");
           });


      AtomicInteger invocationCount = new AtomicInteger();
      cache.getAdvancedCache().getDataContainer().iteratorIncludingExpired().forEachRemaining(ice -> invocationCount.incrementAndGet());
      assertEquals(maxInMemory(), invocationCount.get());

      processExpiration();
      cache.getAdvancedCache().getDataContainer()
           .iteratorIncludingExpired().forEachRemaining(ice -> {
              throw new RuntimeException("No task should be executed on expired entry");
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

   public static class NoEquals implements Serializable {
      private final String value;

      @ProtoFactory
      public NoEquals(String value) {
         this.value = value;
      }

      @ProtoField(number = 1)
      public String getValue() {
         return value;
      }
   }
}
