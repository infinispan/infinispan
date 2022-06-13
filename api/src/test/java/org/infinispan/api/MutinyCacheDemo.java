package org.infinispan.api;

import static org.infinispan.api.common.CacheOptions.options;
import static org.infinispan.api.common.CacheWriteOptions.writeOptions;

import java.time.Duration;
import java.util.Map;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;
import org.infinispan.api.mutiny.MutinyCache;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.operators.uni.UniBlockingAwait;

/**
 * @since 14.0
 **/
public class MutinyCacheDemo {
   public void testAPI() {
      try (Infinispan infinispan = Infinispan.create("file:///path/to/infinispan.xml")) {
         // obtain a cache
         Uni<MutinyCache<String, String>> cacheUni = infinispan.mutiny().caches().get("mycache");
         MutinyCache<String, String> mycache = await(cacheUni);
         // set
         await(mycache.set("key", "value"));
         // get
         String value = await(mycache.get("key"));
         // put
         CacheEntry<String, String> previous = await(mycache.put("key", "newvalue"));
         // set with options
         await(mycache.set("key", "anothervalue", writeOptions().lifespan(Duration.ofHours(1)).timeout(Duration.ofMillis(500)).build()));
         // get with options
         value = await(mycache.get("key", options().timeout(Duration.ofMillis(500)).flags(DemoEnumFlags.of(DemoEnumFlag.skipLoad(), DemoEnumFlag.skipNotification())).build()));
         // get entry
         CacheEntry<String, String> entry = await(mycache.getEntry("key"));
         // query
         mycache.query("age > :age").param("age", 5).skip(5).limit(10).find().subscribe();
         // remove by query
         await(mycache.query("delete from person where age > :age").param("age", 80).skip(5).limit(10).execute());
         // update by query
         mycache.query("age > :age").param("age", 80).skip(5).limit(10).process((entries, ctx) -> null).subscribe();
         // keys
         mycache.keys().subscribe();
         // keys
         mycache.entries().subscribe();
         // Bulk ops
         mycache.putAll(Map.of("key1", "value1", "key2", "value2")).subscribe();
         mycache.getAll("key1", "key2").subscribe();
         // Listen
         mycache.listen(new CacheListenerOptions().clustered(), CacheEntryEventType.CREATED).subscribe();
      }
   }

   public static <T> T await(Uni<T> uni) {
      return UniBlockingAwait.await(uni, Duration.ofHours(1), null);
   }
}
