package org.infinispan.api;

import static org.infinispan.api.common.CacheOptions.options;
import static org.infinispan.api.common.CacheWriteOptions.writeOptions;
import static org.infinispan.api.common.process.CacheProcessorOptions.processorOptions;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.events.cache.SyncCacheEntryCreatedListener;
import org.infinispan.api.sync.events.cache.SyncCacheEntryRemovedListener;
import org.infinispan.api.sync.events.cache.SyncCacheEntryUpdatedListener;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 14.0
 **/
public class SyncCacheAPIDemo {
   public void demo() {
      try (SyncContainer infinispan = Infinispan.create("file:///path/to/infinispan.xml").sync()) {
         // obtain a cache
         SyncCache<String, String> mycache = infinispan.caches().get("mycache");
         // set
         mycache.set("key", "value");
         // get
         String value = mycache.get("key");
         // put
         CacheEntry<String, String> previous = mycache.put("key", "newvalue");
         // set with options
         mycache.set("key", "anothervalue", writeOptions().lifespan(Duration.ofHours(1)).timeout(Duration.ofMillis(500)).build());
         // get with options
         DemoEnumFlags flags = DemoEnumFlags.of(DemoEnumFlag.skipLoad());
         value = mycache.get("key", options().timeout(Duration.ofMillis(500)).flags(flags.add(DemoEnumFlag.skipNotification())).build());
         // get entry
         CacheEntry<String, String> entry = mycache.getEntry("key");
         // setIfAbsent
         mycache.setIfAbsent("anotherkey", "value");
         // putIfAbsent
         CacheEntry<String, String> existing = mycache.putIfAbsent("anotherkey", "anothervalue");
         // remove
         boolean removed = mycache.remove("anotherkey");
         // query
         mycache.query("age > :age").param("age", 5).skip(5).limit(10).find().results().forEach(new NullConsumer<>());
         // remove by query
         mycache.query("delete from person where age > :age").param("age", 80).skip(5).limit(10).execute();
         // process by query
         mycache.query("age > :age").param("age", 80).skip(5).limit(10).process((e, ctx) -> {
            e.setValue(e.value().toLowerCase());
            return null;
         });

         // Bulk ops
         mycache.putAll(Map.of("key1", "value1", "key2", "value2"));
         Map<String, String> all = mycache.getAll(Set.of("key1", "key2"));

         // Iteration over keys and entries
         mycache.keys().forEach(new NullConsumer<>());
         mycache.entries().forEach(e -> System.out.printf("key=%s, value=%s%n", e.key(), e.value()));

         // Event handling
         mycache.listen((SyncCacheEntryCreatedListener<String, String>) event -> {
            // Handle create event
         });

         mycache.listen(new NullListener());

         mycache.process(Set.of("key1", "key2"), (e, ctx) -> {
            e.setValue(e.value().toLowerCase());
            return null;
         }, processorOptions().flags(DemoEnumFlags.of(DemoEnumFlag.skipLoad())).build());

         // Batch
         infinispan.sync().batch(sync -> {
            SyncCache<String, String> c = sync.caches().get("mycache");
            c.set("k1", "v1");
            c.set("k2", "v2");
            return null;
         });
      }
   }

   public static class NullConsumer<T> implements Consumer<T> {

      @Override
      public void accept(T t) {
      }
   }

   public static class NullListener implements SyncCacheEntryUpdatedListener, SyncCacheEntryRemovedListener {

      @Override
      public void onRemove(CacheEntryEvent event) {

      }

      @Override
      public void onUpdate(CacheEntryEvent event) {

      }
   }
}
