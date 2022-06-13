package org.infinispan.api;

import static org.infinispan.api.common.CacheOptions.options;
import static org.infinispan.api.common.CacheWriteOptions.writeOptions;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class ASyncCacheDemo {
   /*
    * Demo of cache API
    */
   public void cache() throws ExecutionException, InterruptedException {
      try (Infinispan infinispan = Infinispan.create("file:///path/to/infinispan.xml")) {
         // obtain a cache
         CompletionStage<? extends AsyncCache<String, String>> cacheCompletionStage = infinispan.async().caches().get("mycache");
         AsyncCache<String, String> mycache = await(cacheCompletionStage);
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
         // setIfAbsent
         await(mycache.setIfAbsent("anotherkey", "value"));
         // putIfAbsent
         CacheEntry<String, String> existing = await(mycache.putIfAbsent("anotherkey", "anothervalue"));
         // remove
         boolean removed = await(mycache.remove("anotherkey"));
         // query
         await(mycache.query("age > :age").param("age", 5).skip(5).limit(10).find()).results().subscribe(new NullSubscriber<>());
         // remove by query
         await(mycache.query("delete from person where age > :age").param("age", 80).skip(5).limit(10).execute());
         // update by query
         mycache.query("age > :age").param("age", 80).skip(5).limit(10).process((e, ctx) -> null).subscribe(new NullSubscriber());
         // keys
         mycache.keys().subscribe(new NullSubscriber<>());
         mycache.entries().subscribe(new NullSubscriber<>());

         mycache.process(Set.of("k1", "k2"), (entries, context) -> null);

         // Bulk ops
         await(mycache.putAll(Map.of("key1", "value1", "key2", "value2")));
         mycache.getAll(Set.of("key1", "key2")).subscribe(new NullSubscriber<>());

         // Listen
         mycache.listen(new CacheListenerOptions().clustered(), CacheEntryEventType.CREATED).subscribe(new NullSubscriber<>());

         // Batch
         await(infinispan.async().batch(async ->
            async.caches().get("mycache").thenCompose(c -> c.set("k1", "v1").thenApply(v -> c)).thenCompose(c -> c.set("k2", "v2"))
         ));
      }
   }

   static class NullSubscriber<T> implements Flow.Subscriber<T> {
      @Override
      public void onSubscribe(Flow.Subscription subscription) {

      }

      @Override
      public void onNext(T item) {

      }

      @Override
      public void onError(Throwable throwable) {

      }

      @Override
      public void onComplete() {

      }
   }

   // Some utilities
   private final static long BIG_DELAY_NANOS = TimeUnit.DAYS.toNanos(1);

   static <T> T await(CompletionStage<T> cf) {
      return await(cf.toCompletableFuture());
   }

   static <T> T await(CompletableFuture<T> cf) {
      try {
         return cf.get(BIG_DELAY_NANOS, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      } catch (ExecutionException e) {
         throw new RuntimeException(e);
      } catch (TimeoutException e) {
         throw new IllegalStateException(e);
      }
   }
}
