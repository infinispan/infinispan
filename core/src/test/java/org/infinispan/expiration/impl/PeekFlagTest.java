package org.infinispan.expiration.impl;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tests that {@link org.infinispan.AdvancedCache#peek(Object)} retrieves a cache entry without resetting the
 * max-idle timer.
 *
 * @author William Burns
 * @since 16.3
 */
@Test(groups = "functional", testName = "expiration.impl.PeekFlagTest")
public class PeekFlagTest extends MultipleCacheManagersTest {

   protected ControlledTimeService ts0;
   protected ControlledTimeService ts1;

   protected Cache<Object, String> cache0;
   protected Cache<Object, String> cache1;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.expiration().disableReaper();
      createCluster(TestDataSCI.INSTANCE, builder, 2);
      waitForClusterToForm();

      ts0 = new ControlledTimeService(address(0));
      TestingUtil.replaceComponent(manager(0), TimeService.class, ts0, true);
      ts1 = new ControlledTimeService(address(1));
      TestingUtil.replaceComponent(manager(1), TimeService.class, ts1, true);

      cache0 = cache(0);
      cache1 = cache(1);
   }

   public void testPeekDoesNotResetIdle() {
      Object key = getKeyForCache(cache0);
      cache0.put(key, "value", -1, MINUTES, 10, MINUTES);

      long initialLastUsed = getLastUsed(cache0, key);

      incrementAllTimeServices(5, MINUTES);

      CacheEntry<Object, String> entry = cache0.getAdvancedCache().peek(key);
      assertNotNull(entry);
      assertEquals("value", entry.getValue());
      assertEquals(initialLastUsed, getLastUsed(cache0, key));

      // Normal get should update lastUsed
      String result = cache0.get(key);
      assertEquals("value", result);
      assertEquals(ts0.wallClockTime(), getLastUsed(cache0, key));
   }

   public void testPeekDistributedDoesNotTouchRemoteNode() {
      Object key = createKey(cache0, cache1);
      cache0.put(key, "value", -1, MINUTES, 10, MINUTES);

      long initialLastUsed = getLastUsed(cache0, key);
      assertEquals(initialLastUsed, getLastUsed(cache1, key));

      incrementAllTimeServices(5, MINUTES);

      CacheEntry<Object, String> entry = cache1.getAdvancedCache().peek(key);
      assertNotNull(entry);
      assertEquals("value", entry.getValue());

      assertEquals(initialLastUsed, getLastUsed(cache0, key));
      assertEquals(initialLastUsed, getLastUsed(cache1, key));

      // Normal get should update lastUsed on both nodes
      String result = cache1.get(key);
      assertEquals("value", result);

      long expectedLastUsed = ts0.wallClockTime();
      assertEquals(expectedLastUsed, getLastUsed(cache0, key));
      assertEquals(expectedLastUsed, getLastUsed(cache1, key));
   }

   public void testPeekReturnsExpiredEntry() {
      Object key = getKeyForCache(cache0);
      cache0.put(key, "value", -1, MINUTES, 10, MINUTES);

      // Advance past max-idle
      incrementAllTimeServices(11, MINUTES);

      // peek should still return the expired entry
      CacheEntry<Object, String> entry = cache0.getAdvancedCache().peek(key);
      assertNotNull(entry);
      assertEquals("value", entry.getValue());

      // Normal get should return null (entry is expired)
      assertNull(cache0.get(key));
   }

   private Object createKey(Cache<Object, String> primaryOwner, Cache<Object, String> backupOwner) {
      LocalizedCacheTopology primaryLct = primaryOwner.getAdvancedCache().getDistributionManager().getCacheTopology();
      LocalizedCacheTopology backupLct = backupOwner.getAdvancedCache().getDistributionManager().getCacheTopology();
      ThreadLocalRandom tlr = ThreadLocalRandom.current();

      int attempt = 0;
      while (true) {
         int key = tlr.nextInt();
         Object wrappedKey = primaryOwner.getAdvancedCache().getKeyDataConversion().toStorage(key);
         if (primaryLct.getDistribution(wrappedKey).isPrimary() &&
               backupLct.getDistribution(wrappedKey).isWriteBackup()) {
            return key;
         }
         if (++attempt == 1_000) {
            throw new AssertionError("Unable to find key that maps to primary " + primaryOwner +
                  " and backup " + backupOwner);
         }
      }
   }

   private void incrementAllTimeServices(long time, TimeUnit unit) {
      for (ControlledTimeService cts : Arrays.asList(ts0, ts1)) {
         cts.advance(unit.toMillis(time));
      }
   }

   private long getLastUsed(Cache<Object, String> cache, Object key) {
      Object storageKey = cache.getAdvancedCache().getKeyDataConversion().toStorage(key);
      InternalCacheEntry<Object, String> entry = cache.getAdvancedCache().getDataContainer().peek(storageKey);
      assertNotNull(entry, "Entry should exist in data container");
      return entry.getLastUsed();
   }
}
