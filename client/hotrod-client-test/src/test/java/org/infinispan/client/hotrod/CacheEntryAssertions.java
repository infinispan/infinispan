package org.infinispan.client.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.client.hotrod.test.KeyValueGenerator;

public final class CacheEntryAssertions {

   private CacheEntryAssertions() { }

   public static  <K, V> void assertEntry(K key, V value, KeyValueGenerator<K, V> kv, CacheEntry<K, V> entry) {
      kv.assertKeyEquals(key, entry.key());
      kv.assertValueEquals(value, entry.value());
      var metadata = entry.metadata();
      if (metadata != null) {
         // the version is random and unable to check.
         assertNotNull(metadata.version());
      }
   }

   public static  <K, V> void assertEntry(K key, V value, KeyValueGenerator<K, V> kv, CacheEntry<K, V> entry,
                                          CacheWriteOptions writeOptions) {
      assertEntry(key, value, kv, entry);
      CacheEntryMetadata metadata = entry.metadata();
      assertEquals(writeOptions.expiration(), metadata.expiration());
   }
}
