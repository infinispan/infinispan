package org.infinispan.api;

import java.util.Map;

import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncQueryResult;
import org.infinispan.api.sync.events.cache.SyncCacheEntryCreatedListener;
import org.infinispan.api.sync.events.cache.SyncCacheEntryRemovedListener;
import org.infinispan.api.sync.events.cache.SyncCacheEntryUpdatedListener;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class SyncCacheAPITest {
   public void testCacheAPI() {
      try (SyncContainer infinispan = Infinispan.create("file:///path/to/infinispan.xml").sync()) {
         SyncCache<String, String> cache = infinispan.caches().get("cache");

         // Simple ops
         cache.put("key", "value");
         cache.putIfAbsent("anotherKey", "anotherValue");
         String value = cache.get("key");
         cache.remove("key");

         // Compute
         cache.compute("key", (k, v) -> "value");
         cache.computeIfAbsent("key", (k) -> "value");

         // Bulk ops
         cache.putAll(Map.of("key1", "value1", "key2", "value2"));

         // Iteration over keys and entries
         cache.keys().forEach(k -> System.out.printf("key=%s%n", k));
         cache.entries().forEach(e -> System.out.printf("key=%s, value=%s%n", e.key(), e.value()));

         // Query
         Iterable<SyncQueryResult<Object>> results = cache.find("%alu%");

         // Parameterized query
         results = cache.query("...").skip(10).limit(100).param("a", "b").find();

         // Event handling
         cache.listen((SyncCacheEntryCreatedListener<String, String>) event -> {
            // Handle create event
         });

         cache.listen(new AListener());
      }
   }

   public static class AListener implements SyncCacheEntryUpdatedListener, SyncCacheEntryRemovedListener {

      @Override
      public void onRemove(CacheEntryEvent event) {

      }

      @Override
      public void onUpdate(CacheEntryEvent event) {

      }
   }
}
