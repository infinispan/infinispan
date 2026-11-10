package org.infinispan.xsite.irac;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.annotations.Test;

/**
 * Tests the asynchronous cross-site replication of a cache with eviction.
 * <p>
 * An evicted entry does not store a tombstone. If the cache has no store, the entry cannot be loaded back and the
 * update cannot be sent to the remote site. Those keys must be discarded instead of being retried forever. If the
 * cache has a store with passivation, the entry is loaded back from the store and it must be replicated.
 *
 * @since 16.3
 */
@Test(groups = "xsite", testName = "xsite.irac.IracEvictionTest")
public class IracEvictionTest extends AbstractMultipleSitesTest {

   private static final int MAX_COUNT = 10;
   private static final int NUMBER_OF_KEYS = 50;
   private static final String NO_STORE_CACHE = "evict-without-store";
   private static final String PASSIVATION_CACHE = "evict-with-passivation";

   @Override
   protected int defaultNumberOfNodes() {
      return 1;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = super.defaultConfigurationForSite(siteIndex);
      builder.sites().addBackup()
            .site(siteIndex == 0 ? siteName(1) : siteName(0))
            .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      return builder;
   }

   @Override
   protected void afterSitesCreated() {
      ConfigurationBuilder evictionBuilder = new ConfigurationBuilder();
      evictionBuilder.memory().maxCount(MAX_COUNT);
      startCache(siteName(0), NO_STORE_CACHE, evictionBuilder);
      startCache(siteName(1), NO_STORE_CACHE, new ConfigurationBuilder());

      ConfigurationBuilder passivationBuilder = new ConfigurationBuilder();
      passivationBuilder.memory().maxCount(MAX_COUNT);
      passivationBuilder.persistence().passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .purgeOnStartup(true);
      startCache(siteName(0), PASSIVATION_CACHE, passivationBuilder);
      startCache(siteName(1), PASSIVATION_CACHE, new ConfigurationBuilder());
   }

   /**
    * Without a store, the evicted entries can never be sent to the remote site. They must be discarded and the
    * remaining keys must be replicated.
    */
   public void testEvictedKeysWithoutStore() {
      Cache<String, String> cache = cache(0, 0, NO_STORE_CACHE);
      writeKeysWhileReplicationIsBlocked(cache);

      Set<String> keysInMemory = new HashSet<>(cache.keySet());
      assertFalse(keysInMemory.isEmpty(), "All the keys were evicted");
      assertTrue(keysInMemory.size() < NUMBER_OF_KEYS, "No key was evicted");

      assertEventuallyInSite(siteName(1), NO_STORE_CACHE,
            backupCache -> backupCache.keySet().containsAll(keysInMemory), 30, TimeUnit.SECONDS);
      assertNoDataLeak(NO_STORE_CACHE);
   }

   /**
    * With passivation, the evicted entries are loaded back from the store. No key can be discarded.
    */
   public void testEvictedKeysWithPassivation() {
      Cache<String, String> cache = cache(0, 0, PASSIVATION_CACHE);
      writeKeysWhileReplicationIsBlocked(cache);

      assertEventuallyInSite(siteName(1), PASSIVATION_CACHE, this::containsAllKeys, 30, TimeUnit.SECONDS);
      assertNoDataLeak(PASSIVATION_CACHE);
   }

   /**
    * Writes the keys while the {@link IracManager} is not sending anything, so the entries can be evicted before the
    * update is sent to the remote site.
    */
   private void writeKeysWhileReplicationIsBlocked(Cache<String, String> cache) {
      ManualIracManager iracManager = ManualIracManager.wrapCache(cache);
      iracManager.enable();
      try {
         for (int i = 0; i < NUMBER_OF_KEYS; ++i) {
            cache.put(key(i), value(i));
         }
      } finally {
         iracManager.disable(ManualIracManager.DisableMode.SEND);
      }
   }

   private boolean containsAllKeys(Cache<String, String> cache) {
      for (int i = 0; i < NUMBER_OF_KEYS; ++i) {
         if (!value(i).equals(cache.get(key(i)))) {
            return false;
         }
      }
      return true;
   }

   private static String key(int index) {
      return "key-" + index;
   }

   private static String value(int index) {
      return "value-" + index;
   }
}
