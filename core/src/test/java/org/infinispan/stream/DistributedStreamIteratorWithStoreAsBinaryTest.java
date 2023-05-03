package org.infinispan.stream;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.AutoProtoSchemaBuilder;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Test to verify distributed entry behavior when store as binary is used
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.DistributedStreamIteratorWithStoreAsBinaryTest")
@InCacheMode({ CacheMode.DIST_SYNC })
public class DistributedStreamIteratorWithStoreAsBinaryTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      builderUsed.clustering().hash().numOwners(1);
      builderUsed.memory().storageType(StorageType.BINARY);
      createClusteredCaches(3, new StreamStoreAsBinarySerializationContextImpl(), builderUsed);
   }

   @Test
   public void testFilterWithStoreAsBinary() {
      Cache<MagicKey, String> cache0 = cache(0);
      Cache<MagicKey, String> cache1 = cache(1);
      Cache<MagicKey, String> cache2 = cache(2);

      Map<MagicKey, String> originalValues = new HashMap<>();
      originalValues.put(new MagicKey(cache0), "cache0");
      originalValues.put(new MagicKey(cache1), "cache1");
      originalValues.put(new MagicKey(cache2), "cache2");

      cache0.putAll(originalValues);

      // Try filter for all values
      Iterator<CacheEntry<MagicKey, String>> iterator = cache1.getAdvancedCache().cacheEntrySet().stream().
              filter(CacheFilters.predicate(new MagicKeyStringFilter(originalValues))).iterator();

      // we need this count since the map will replace same key'd value
      int count = 0;
      Map<MagicKey, String> results = new HashMap<MagicKey, String>();
      while (iterator.hasNext()) {
         Map.Entry<MagicKey, String> entry = iterator.next();
         results.put(entry.getKey(), entry.getValue());
         count++;
      }
      assertEquals(count, 3);
      assertEquals(originalValues, results);
   }

   @Test
   public void testFilterWithStoreAsBinaryPartialKeys() {
      Cache<MagicKey, String> cache0 = cache(0);
      Cache<MagicKey, String> cache1 = cache(1);
      Cache<MagicKey, String> cache2 = cache(2);

      MagicKey findKey = new MagicKey(cache1);
      Map<MagicKey, String> originalValues = new HashMap<>();
      originalValues.put(new MagicKey(cache0), "cache0");
      originalValues.put(findKey, "cache1");
      originalValues.put(new MagicKey(cache2), "cache2");

      cache0.putAll(originalValues);

      // Try filter for all values
      Iterator<CacheEntry<MagicKey, String>> iterator = cache1.getAdvancedCache().cacheEntrySet().stream().
              filter(CacheFilters.predicate(new MagicKeyStringFilter(Collections.singletonMap(findKey, "cache1")))).iterator();

      CacheEntry<MagicKey, String> entry = iterator.next();
      AssertJUnit.assertEquals(findKey, entry.getKey());
      AssertJUnit.assertEquals("cache1", entry.getValue());
      assertFalse(iterator.hasNext());
   }

   static class MagicKeyStringFilter implements KeyValueFilter<MagicKey, String> {

      Map<MagicKey, String> allowedEntries;

      MagicKeyStringFilter() {}

      MagicKeyStringFilter(Map<MagicKey, String> allowedEntries) {
         this.allowedEntries = allowedEntries;
      }

      @ProtoField(number = 1, collectionImplementation = ArrayList.class)
      public List<MapPair> getMapEntries() {
         return allowedEntries.entrySet().stream().map(MapPair::new).collect(Collectors.toCollection(ArrayList::new));
      }

      public void setMapEntries(List<MapPair> entries) {
         this.allowedEntries = entries.stream().collect(Collectors.toMap(m -> m.key, m -> m.value));
      }

      @Override
      public boolean accept(MagicKey key, String value, Metadata metadata) {
         String allowedValue = allowedEntries.get(key);
         return allowedValue != null && allowedValue.equals(value);
      }
   }

   static class MapPair {

      @ProtoField(1)
      MagicKey key;

      @ProtoField(2)
      String value;

      MapPair() {}

      MapPair(Map.Entry<MagicKey, String> entry) {
         this.key = entry.getKey();
         this.value = entry.getValue();
      }
   }

   @AutoProtoSchemaBuilder(
         includeClasses = {
               MagicKey.class,
               MagicKeyStringFilter.class,
               MapPair.class,
         },
         schemaFileName = "core.stream.binary.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.stream.binary",
         service = false
   )
   interface StreamStoreAsBinarySerializationContext extends SerializationContextInitializer {
   }
}
