package org.infinispan.persistence;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.testng.AssertJUnit.assertArrayEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.NonBlockingStore;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.WaitDelegatingNonBlockingStore;
import org.infinispan.persistence.support.WaitNonBlockingStore;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.test.Mocks;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.data.Address;
import org.infinispan.test.data.Person;
import org.infinispan.test.data.Sex;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * This is a base functional test class containing tests that should be executed for each cache store/loader
 * implementation. As these are functional tests, they should interact against Cache/CacheManager only and any access to
 * the underlying cache store/loader should be done to verify contents.
 */
@Test(groups = {"unit", "smoke"}, testName = "persistence.BaseStoreFunctionalTest")
public abstract class BaseStoreFunctionalTest extends SingleCacheManagerTest {

   private static final SerializationContextInitializer CONTEXT_INITIALIZER = TestDataSCI.INSTANCE;
   protected final ControlledTimeService timeService = new ControlledTimeService();

   protected abstract PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload);


   protected ConfigurationBuilder getDefaultCacheConfiguration() {
      return TestCacheManagerFactory.getDefaultCacheConfiguration(false);
   }

   protected Object wrap(String key, String value) {
      return value;
   }

   protected String unwrap(Object wrapped) {
      return (String) wrapped;
   }

   protected BaseStoreFunctionalTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory(this.getClass()));
      global.serialization().addContextInitializer(getSerializationContextInitializer());
      global.cacheContainer().security().authorization().enable();
      return createCacheManager(false, global, new ConfigurationBuilder());
   }

   protected EmbeddedCacheManager createCacheManager(boolean start, GlobalConfigurationBuilder global, ConfigurationBuilder cb) {
      return TestCacheManagerFactory.newDefaultCacheManager(start, global, cb);
   }

   protected SerializationContextInitializer getSerializationContextInitializer() {
      return CONTEXT_INITIALIZER;
   }

   protected void assertPersonEqual(Person firstPerson, Person secondPerson) {
      assertEquals(firstPerson, secondPerson);
   }

   public void testTwoCachesSameCacheStore() {
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), "testTwoCachesSameCacheStore", false);
      Configuration c = cb.build();
      TestingUtil.defineConfiguration(cacheManager, "testTwoCachesSameCacheStore-1", c);
      TestingUtil.defineConfiguration(cacheManager, "testTwoCachesSameCacheStore-2", c);

      Cache<String, Object> first = cacheManager.getCache("testTwoCachesSameCacheStore-1");
      Cache<String, Object> second = cacheManager.getCache("testTwoCachesSameCacheStore-2");

      first.start();
      second.start();

      first.put("key", wrap("key", "val"));
      assertEquals("val", unwrap(first.get("key")));
      assertNull(second.get("key"));

      second.put("key2", wrap("key2", "val2"));
      assertEquals("val2", unwrap(second.get("key2")));
      assertNull(first.get("key2"));
   }

   public void testPreloadAndExpiry() {
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), "testPreloadAndExpiry", true);
      TestingUtil.defineConfiguration(cacheManager, "testPreloadAndExpiry", cb.build());
      Cache<String, Object> cache = cacheManager.getCache("testPreloadAndExpiry");
      cache.start();

      assert cache.getCacheConfiguration().persistence().preload();

      cache.put("k1", wrap("k1", "v"));
      cache.put("k2", wrap("k2", "v"), 111111, TimeUnit.MILLISECONDS);
      cache.put("k3", wrap("k3", "v"), -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
      cache.put("k4", wrap("k4", "v"), 444444, TimeUnit.MILLISECONDS, 333333, TimeUnit.MILLISECONDS);

      assertCacheEntry(cache, "k1", "v", -1, -1);
      assertCacheEntry(cache, "k2", "v", 111111, -1);
      assertCacheEntry(cache, "k3", "v", -1, 222222);
      assertCacheEntry(cache, "k4", "v", 444444, 333333);
      cache.stop();

      cache.start();

      assertCacheEntry(cache, "k1", "v", -1, -1);
      assertCacheEntry(cache, "k2", "v", 111111, -1);
      assertCacheEntry(cache, "k3", "v", -1, 222222);
      assertCacheEntry(cache, "k4", "v", 444444, 333333);
   }

   public void testPreloadStoredAsBinary() {
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), "testPreloadStoredAsBinary", true).memory().storageType(StorageType.BINARY);
      TestingUtil.defineConfiguration(cacheManager, "testPreloadStoredAsBinary", cb.build());
      Cache<String, Person> cache = cacheManager.getCache("testPreloadStoredAsBinary");
      cache.start();

      assert cache.getCacheConfiguration().persistence().preload();
      assertEquals(StorageType.BINARY, cache.getCacheConfiguration().memory().storageType());

      byte[] pictureBytes = new byte[]{1, 82, 123, 19};

      cache.put("k1", createEmptyPerson("1"));
      cache.put("k2", new Person("2", null, pictureBytes, null, null, false, 4.6, 5.6f, 8.4, 9.2f), 111111, TimeUnit.MILLISECONDS);
      cache.put("k3", new Person("3", null, null, Sex.MALE, null, false, 4.7, 5.7f, 8.5, 9.3f), -1, TimeUnit.MILLISECONDS, 222222, TimeUnit.MILLISECONDS);
      cache.put("k4", new Person("4", new Address("Street", "City", 12345), null, null, null, true, 4.8, 5.8f, 8.6, 9.4f), 333333, TimeUnit.MILLISECONDS, 444444, TimeUnit.MILLISECONDS);
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("EST"));
      calendar.set(2009, Calendar.MARCH, 18, 3, 22, 57);
      // Chop off the last milliseconds as some databases don't have that high of accuracy
      calendar.setTimeInMillis((calendar.getTimeInMillis() / 1000) * 1000);
      Date infinispanBirthDate = calendar.getTime();
      Person infinispanPerson = createEmptyPerson("Infinispan");
      infinispanPerson.setBirthDate(infinispanBirthDate);
      infinispanPerson.setAcceptedToS(true);
      cache.put("Infinispan", infinispanPerson);

      // Just to prove nothing in memory even with stop
      cache.getAdvancedCache().getDataContainer().clear();

      assertPersonEqual(createEmptyPerson("1"), cache.get("k1"));
      Person person2 = createEmptyPerson("2");
      person2.setPicture(pictureBytes);
      person2.setMoneyOwned(4.6);
      person2.setMoneyOwed(5.6f);
      person2.setDecimalField(8.4);
      person2.setRealField(9.2f);
      assertPersonEqual(person2, cache.get("k2"));
      Person person3 = createEmptyPerson("3");
      person3.setSex(Sex.MALE);
      person3.setMoneyOwned(4.7);
      person3.setMoneyOwed(5.7f);
      person3.setDecimalField(8.5);
      person3.setRealField(9.3f);
      assertPersonEqual(person3, cache.get("k3"));
      assertPersonEqual(new Person("4", new Address("Street", "City", 12345), null, null, null, true, 4.8, 5.8f, 8.6, 9.4f), cache.get("k4"));
      assertPersonEqual(infinispanPerson, cache.get("Infinispan"));

      cache.stop();

      cache.start();

      assertEquals(5, cache.entrySet().size());
      assertPersonEqual(createEmptyPerson("1"), cache.get("k1"));
      assertPersonEqual(person2, cache.get("k2"));
      assertPersonEqual(person3, cache.get("k3"));
      assertPersonEqual(new Person("4", new Address("Street", "City", 12345), null, null, null, true, 4.8, 5.8f, 8.6, 9.4f), cache.get("k4"));
      assertPersonEqual(infinispanPerson, cache.get("Infinispan"));
   }

   protected Person createEmptyPerson(String name) {
      return new Person(name);
   }

   public void testStoreByteArrays(final Method m) throws PersistenceException {
      ConfigurationBuilder base = getDefaultCacheConfiguration();
      // we need to purge the container when loading, because we could try to compare
      // some old entry using ByteArrayEquivalence and this throws ClassCastException
      // for non-byte[] arguments
      TestingUtil.defineConfiguration(cacheManager, m.getName(), configureCacheLoader(base, m.getName(), true).build());
      Cache<byte[], byte[]> cache = cacheManager.getCache(m.getName());
      byte[] key = {1, 2, 3};
      byte[] value = {4, 5, 6};
      cache.put(key, value);
      // Lookup in memory, sanity check
      byte[] lookupKey = {1, 2, 3};
      byte[] found = cache.get(lookupKey);
      assertNotNull(found);
      assertArrayEquals(value, found);
      cache.evict(key);
      // Lookup in cache store
      found = cache.get(lookupKey);
      assertNotNull(found);
      assertArrayEquals(value, found);
   }

   public void testRemoveCache() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory(this.getClass()));
      global.serialization().addContextInitializer(getSerializationContextInitializer());
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      final String cacheName = "testRemoveCache";
      createCacheStoreConfig(cb.persistence(), cacheName, true);
      EmbeddedCacheManager local = createCacheManager(true, global, cb);
      try {
         local.defineConfiguration(cacheName, local.getDefaultCacheConfiguration());
         Cache<String, Object> cache = local.getCache(cacheName);
         assertTrue(local.isRunning(cacheName));
         cache.put("1", wrap("1", "v1"));
         assertCacheEntry(cache, "1", "v1", -1, -1);
         local.administration().removeCache(cacheName);
         assertFalse(local.isRunning(cacheName));
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

   public void testRemoveCacheWithPassivation() {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory(this.getClass()));
      global.serialization().addContextInitializer(getSerializationContextInitializer());
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      final String cacheName = "testRemoveCacheWithPassivation";
      createCacheStoreConfig(cb.persistence().passivation(true), cacheName, true);
      EmbeddedCacheManager local = createCacheManager(true, global, cb);
      try {
         local.defineConfiguration(cacheName, local.getDefaultCacheConfiguration());
         Cache<String, Object> cache = local.getCache(cacheName);
         assertTrue(local.isRunning(cacheName));
         cache.put("1", wrap("1", "v1"));
         assertCacheEntry(cache, "1", "v1", -1, -1);
         ByRef<Boolean> passivate = new ByRef<>(false);
         PersistenceManager actual = ComponentRegistry.componentOf(cache, PersistenceManager.class);
         PersistenceManager stub = new TrackingPersistenceManager(actual, passivate);
         TestingUtil.replaceComponent(cache, PersistenceManager.class, stub, true);
         local.administration().removeCache(cacheName);
         assertFalse(local.isRunning(cacheName));
         assertFalse(passivate.get());
      } finally {
         TestingUtil.killCacheManagers(local);
      }
   }

   public void testPutAllBatch() {
      int numberOfEntries = 100;
      String cacheName = "testPutAllBatch";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);
      Map<String, Object> entriesMap = IntStream.range(0, numberOfEntries).boxed()
            .collect(Collectors.toMap(Object::toString, i -> wrap(i.toString(), "Val" + i)));
      cache.putAll(entriesMap);

      assertEquals(numberOfEntries, cache.size());
      WaitNonBlockingStore<String, Object> store = TestingUtil.getFirstStoreWait(cache);
      for (int i = 0; i < numberOfEntries; ++i) {
         assertNotNull("Entry for key: " + i + " was null", store.loadEntry(toStorage(cache, Integer.toString(i))));
      }
   }

   public void testIterator() {
      int numberOfEntries = 100;
      String cacheName = "testIterator";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);
      Map<String, Object> entriesMap = IntStream.range(0, numberOfEntries).boxed()
            .collect(Collectors.toMap(Object::toString, i -> wrap(i.toString(), "Val" + i)));
      cache.putAll(entriesMap);

      Map<String, Object> results = new HashMap<>(numberOfEntries);
      try (CloseableIterator<Map.Entry<String, Object>> iter = cache.getAdvancedCache().entrySet().iterator()) {
         iter.forEachRemaining(e -> results.put(e.getKey(), e.getValue()));
      }

      assertEquals(entriesMap, results);
   }

   public void testIteratorWithSegment() {
      int numberOfEntries = 100;
      String cacheName = "testIteratorWithSegment";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      Cache<String, Object> cache = cacheManager.getCache(cacheName);
      IntSet segments = IntSets.mutableEmptySet();
      for (int i = 0; i < cache.getCacheConfiguration().clustering().hash().numSegments(); ++i) {
         if (i % 2 == 0) {
            segments.set(i);
         }
      }

      Map<String, Object> entriesMap = IntStream.range(0, numberOfEntries).boxed()
            .collect(Collectors.toMap(Object::toString, i -> wrap(i.toString(), "Val" + i)));
      cache.putAll(entriesMap);

      KeyPartitioner kp = TestingUtil.extractComponent(cache, KeyPartitioner.class);

      Map<String, Object> targetMap = entriesMap.entrySet().stream().filter(e ->
            segments.contains(kp.getSegment(e.getKey()))
      ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      Map<String, Object> results = new HashMap<>(numberOfEntries);
      try (CacheStream<Map.Entry<String, Object>> stream = cache.getAdvancedCache().entrySet().stream()) {
         stream.filterKeySegments(segments).iterator().forEachRemaining(e -> results.put(e.getKey(), e.getValue()));
      }

      assertEquals(targetMap, results);
   }

   Object fromStorage(Cache<?, ?> cache, Object value) {
      return cache.getAdvancedCache().getValueDataConversion()
            .withRequestMediaType(MediaType.APPLICATION_OBJECT)
            .fromStorage(value);
   }

   Object toStorage(Cache<?, ?> cache, Object key) {
      return cache.getAdvancedCache().getKeyDataConversion()
            .withRequestMediaType(MediaType.APPLICATION_OBJECT)
            .toStorage(key);
   }

   public void testLoadEntrySet() {
      int numberOfEntries = 10;
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), "testLoadEntrySet", true);
      Configuration configuration = cb.build();
      TestingUtil.defineConfiguration(cacheManager, "testLoadEntrySet", configuration);
      Cache<String, Object> cache = cacheManager.getCache("testLoadEntrySet");

      Map<String, Object> entriesMap = IntStream.range(0, numberOfEntries).boxed()
            .collect(Collectors.toMap(Object::toString, i -> wrap(i.toString(), "Val" + i)));
      cache.putAll(entriesMap);

      cache.stop();
      cache.start();

      CacheSet<Map.Entry<String, Object>> set = cache.entrySet();
      assertEquals(numberOfEntries, cache.size());
      assertEquals(numberOfEntries, set.size());
      set.forEach(e -> assertEquals(entriesMap.get(e.getKey()), e.getValue()));
   }

   public void testReloadWithEviction() {
      int numberOfEntries = 10;
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      createCacheStoreConfig(cb.persistence(), "testReloadWithEviction", false).memory().size(numberOfEntries / 2).evictionType(EvictionType.COUNT);
      TestingUtil.defineConfiguration(cacheManager, "testReloadWithEviction", cb.build());
      Cache<String, Object> cache = cacheManager.getCache("testReloadWithEviction");

      Map<String, Object> entriesMap = IntStream.range(0, numberOfEntries).boxed()
            .collect(Collectors.toMap(Object::toString, i -> wrap(i.toString(), "Val" + i)));
      cache.putAll(entriesMap);

      assertEquals(numberOfEntries, cache.size());
      entriesMap.forEach((k, v) -> assertEquals(v, cache.get(k)));

      cache.stop();
      cache.start();

      assertEquals(numberOfEntries, cache.size());
      entriesMap.forEach((k, v) -> assertEquals(v, cache.get(k)));
   }

   protected ConfigurationBuilder configureCacheLoader(ConfigurationBuilder base, String cacheName,
         boolean purge) {
      ConfigurationBuilder cfg = base == null ? getDefaultCacheConfiguration() : base;
      cfg.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createCacheStoreConfig(cfg.persistence(), cacheName, false);
      cfg.persistence().stores().get(0).purgeOnStartup(purge);
      return cfg;
   }

   private void assertCacheEntry(Cache cache, String key, String value, long lifespanMillis, long maxIdleMillis) {
      DataContainer dc = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry ice = dc.peek(toStorage(cache, key));
      assertNotNull(ice);
      assertEquals(value, unwrap(fromStorage(cache, ice.getValue())));
      assertEquals(lifespanMillis, ice.getLifespan());
      assertEquals(maxIdleMillis, ice.getMaxIdle());
      if (lifespanMillis > -1) assert ice.getCreated() > -1 : "Lifespan is set but created time is not";
      if (maxIdleMillis > -1) assert ice.getLastUsed() > -1 : "Max idle is set but last used is not";
   }

   @SurvivesRestarts
   static class TrackingPersistenceManager extends org.infinispan.persistence.support.DelegatingPersistenceManager {
      private final ByRef<Boolean> passivate;

      public TrackingPersistenceManager(PersistenceManager actual, ByRef<Boolean> passivate) {
         super(actual);
         this.passivate = passivate;
      }

      @Override
      public void start() {
         // Do nothing, the actual PersistenceManager is already started when it is wrapped
      }

      @Override
      public <K, V> CompletionStage<Void> writeEntries(Iterable<MarshallableEntry<K, V>> iterable,
            Predicate<? super StoreConfiguration> predicate) {
         passivate.set(true);
         return super.writeEntries(iterable, predicate);
      }
   }

   public void testPurgeWithConcurrentUpdate() throws InterruptedException, TimeoutException, ExecutionException {
      String cacheName = "testPurgeWithConcurrentUpdate";
      ConfigurationBuilder cb = getDefaultCacheConfiguration();
      if (cb.clustering().cacheMode() != CacheMode.LOCAL) {
         // Test only operates properly in a LOCAL cache mode
         return;
      }
      cb.expiration().wakeUpInterval(Long.MAX_VALUE);
      createCacheStoreConfig(cb.persistence(), cacheName, false);
      TestingUtil.defineConfiguration(cacheManager, cacheName, cb.build());

      TestingUtil.replaceComponent(cacheManager, TimeService.class, timeService, true);

      Cache<String, Object> cache = cacheManager.getCache(cacheName);

      WaitDelegatingNonBlockingStore store = TestingUtil.getFirstStoreWait(cache);
      if (!store.characteristics().contains(NonBlockingStore.Characteristic.EXPIRATION)) {
         // This test doesn't work if the store doesn't have expiration
         return;
      }

      cache.put("expired-key", "expired-value", 10, TimeUnit.MILLISECONDS);
      timeService.advance(11);

      CheckPoint putCheckpoint = new CheckPoint();
      putCheckpoint.triggerForever(Mocks.AFTER_RELEASE);

      // The put should cause the store to remove the entry as it has expired
      PersistenceManager original = Mocks.blockingMock(putCheckpoint, PersistenceManager.class, cache,
            ((stub, m) -> stub.when(m).deleteFromAllStores(any(), anyInt(), any())));
      try {

         Future<Object> putFuture = fork(() -> cache.put("expired-key", "non-expired-value", 10, TimeUnit.HOURS));

         putCheckpoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

         CheckPoint expirationCheckPoint = new CheckPoint();
         expirationCheckPoint.triggerForever(Mocks.AFTER_RELEASE);
         expirationCheckPoint.triggerForever(Mocks.BEFORE_RELEASE);

         // We cannot mock the InternalEntryFactory as the PeristenceManager invokes it and thus it won't be replaced properly
         Mocks.blockingMock(expirationCheckPoint, InternalDataContainer.class, cache,
               ((stub, m) -> stub.when(m).compute(any(), any())));

         Future<Void> expirationFuture = fork(() ->
               CompletionStages.join(TestingUtil.extractComponent(cache, PersistenceManager.class).purgeExpired()));

         expirationCheckPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

         putCheckpoint.triggerForever(Mocks.BEFORE_RELEASE);

         putFuture.get(10, TimeUnit.SECONDS);
         expirationFuture.get(10, TimeUnit.SECONDS);
      } finally {
         // Some Stores may hold a lock so we can't replace the component back, instead we directly shut it down
         original.stop();
      }
   }
}
