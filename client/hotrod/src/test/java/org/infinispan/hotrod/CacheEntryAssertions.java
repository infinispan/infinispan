package org.infinispan.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.test.KeyValueGenerator;

public final class CacheEntryAssertions {

   private CacheEntryAssertions() { }

   public static  <K, V> void assertEntry(K key, V value, KeyValueGenerator<K, V> kv, CacheEntry<K, V> entry) {
      kv.assertKeyEquals(key, entry.key());
      kv.assertValueEquals(value, entry.value());
   }

   public static  <K, V> void assertEntry(K key, V value, KeyValueGenerator<K, V> kv, CacheEntry<K, V> entry,
                                          CacheWriteOptions writeOptions) {
      assertEntry(key, value, kv, entry);
      CacheEntryMetadata metadata = entry.metadata();
      assertEquals(writeOptions.expiration(), metadata.expiration());
   }

   public static  <K, V> void assertEntry(K key, V value, KeyValueGenerator<K, V> kv, CacheEntry<K, V> entry,
                                          CacheWriteOptions writeOptions, CacheEntryVersion version) {
      assertEntry(key, value, kv, entry, writeOptions);
      assertEquals(version, entry.metadata().version());
   }
}
