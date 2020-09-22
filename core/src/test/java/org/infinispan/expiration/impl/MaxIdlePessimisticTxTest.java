package org.infinispan.expiration.impl;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.Flag;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.Test;

/**
 * Test to verify clustered max idle in a pessimistic transaction
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@Test(groups = "functional", testName = "expiration.impl.MaxIdlePessimisticTxTest")
public class MaxIdlePessimisticTxTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 3;
   private static final long MAX_IDLE = 10;
   private final ControlledTimeService timeService = new ControlledTimeService();

   public void testWriteLock(Method method) throws Exception {
      final String key = k(method);
      final String value = v(method, 0);
      final String value2 = v(method, 1);


      Cache<String, String> cache = findNonOwnerCache(key);
      cache.put(key, value, -1, TimeUnit.SECONDS, MAX_IDLE, TimeUnit.MILLISECONDS);

      long lastWallClock = timeService.wallClockTime();
      timeService.advance(MAX_IDLE - 1);

      assertNotExpired(key);
      assertLastUsed(key, lastWallClock);

      cache.getAdvancedCache().getTransactionManager().begin();
      assertEquals(value, cache.put(key, value2, -1, TimeUnit.SECONDS, MAX_IDLE, TimeUnit.MILLISECONDS));
      assertEquals(value2, cache.get(key));
      // rollback the transaction
      cache.getAdvancedCache().getTransactionManager().rollback();

      lastWallClock = timeService.wallClockTime();
      assertNotExpired(key);
      assertLastUsed(key, lastWallClock);

      // the tx touches the key, this advance should not expire it
      timeService.advance(2);
      assertNotExpired(key);
      assertLastUsed(key, lastWallClock);

      // expire the key
      timeService.advance(MAX_IDLE);
      assertExpired(key);
      assertLastUsed(key, lastWallClock);

      assertNull(cache(0).get(key));
   }

   public void testReadLock(Method method) throws Exception {
      final String key = k(method);
      final String value = v(method, 0);


      Cache<String, String> cache = findNonOwnerCache(key);
      cache.put(key, value, -1, TimeUnit.SECONDS, MAX_IDLE, TimeUnit.MILLISECONDS);

      long lastWallClock = timeService.wallClockTime();
      timeService.advance(MAX_IDLE - 1);

      assertNotExpired(key);
      assertLastUsed(key, lastWallClock);

      cache.getAdvancedCache().getTransactionManager().begin();
      assertEquals(value, cache.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key));
      // rollback the transaction
      cache.getAdvancedCache().getTransactionManager().rollback();

      lastWallClock = timeService.wallClockTime();
      assertNotExpired(key);
      assertLastUsed(key, lastWallClock);

      // the tx touches the key, this advance should not expire it
      timeService.advance(2);
      assertNotExpired(key);
      assertLastUsed(key, lastWallClock);

      // expire the key
      timeService.advance(MAX_IDLE);
      assertExpired(key);
      assertLastUsed(key, lastWallClock);

      assertNull(cache(0).get(key));
   }

   public void testReadLockExpired(Method method) throws Exception {
      final String key = k(method);
      final String value = v(method, 0);


      Cache<String, String> cache = findNonOwnerCache(key);
      cache.put(key, value, -1, TimeUnit.SECONDS, MAX_IDLE, TimeUnit.MILLISECONDS);

      long lastWallClock = timeService.wallClockTime();
      timeService.advance(MAX_IDLE + 1);

      assertExpired(key);
      assertLastUsed(key, lastWallClock);

      cache.getAdvancedCache().getTransactionManager().begin();
      assertNull(cache.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key));
      // rollback the transaction
      cache.getAdvancedCache().getTransactionManager().rollback();

      assertExpired(key);
      assertLastUsed(key, lastWallClock);

      assertNull(cache(0).get(key));
   }

   public void testWriteLockExpired(Method method) throws Exception {
      final String key = k(method);
      final String value = v(method, 0);
      final String value2 = v(method, 1);


      Cache<String, String> cache = findNonOwnerCache(key);
      cache.put(key, value, -1, TimeUnit.SECONDS, MAX_IDLE, TimeUnit.MILLISECONDS);

      long lastWallClock = timeService.wallClockTime();
      timeService.advance(MAX_IDLE + 1);

      assertExpired(key);
      assertLastUsed(key, lastWallClock);

      cache.getAdvancedCache().getTransactionManager().begin();
      assertNull(cache.put(key, value2, -1, TimeUnit.SECONDS, MAX_IDLE, TimeUnit.MILLISECONDS));
      assertEquals(value2, cache.get(key));
      // rollback the transaction
      cache.getAdvancedCache().getTransactionManager().rollback();

      lastWallClock = timeService.wallClockTime();
      assertExpired(key);
      assertLastUsed(key, lastWallClock);

      assertNull(cache(0).get(key));
   }

   private Cache<String, String> findNonOwnerCache(String key) {
      for (Cache<String, String> cache : this.<String, String>caches()) {
         if (!extractComponent(cache, ClusteringDependentLogic.class).getCacheTopology().isReadOwner(key)) {
            return cache;
         }
      }
      fail();
      throw new IllegalStateException();
   }

   private void assertNotExpired(String key) {
      getKeyFromAllCaches(key).forEach(entry -> assertFalse(entry.isExpired(timeService.wallClockTime())));
   }

   private void assertLastUsed(String key, long expected) {
      getKeyFromAllCaches(key).forEach(entry -> assertEquals(expected, entry.getLastUsed()));
   }

   private void assertExpired(String key) {
      getKeyFromAllCaches(key).forEach(entry -> assertTrue(entry.isExpired(timeService.wallClockTime())));
   }

   private Stream<? extends InternalCacheEntry<?, ?>> getKeyFromAllCaches(String key) {
      return caches().stream().map(cache -> {
         InternalDataContainer<?, ?> dc = extractComponent(cache, InternalDataContainer.class);
         KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
         return dc.peek(keyPartitioner.getSegment(key), key);
      }).filter(Objects::nonNull);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      createCluster(builder, NUM_NODES);
      cacheManagers.forEach(cm -> replaceComponent(cm, TimeService.class, timeService, true));
   }
}
