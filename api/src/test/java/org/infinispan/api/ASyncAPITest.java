package org.infinispan.api;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class ASyncAPITest {
   public void testAPI() throws ExecutionException, InterruptedException {
      try (Infinispan infinispan = Infinispan.create("file:///path/to/infinispan.xml")) {
         CompletionStage<AsyncCache<String, String>> cacheCompletionStage = infinispan.async().caches().get("mycache");
         AsyncCache<String, String> mycache = cacheCompletionStage.toCompletableFuture().get();
         mycache.put("key", "value").toCompletableFuture().get();
         mycache.keys().subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
               subscription.request(1000);
            }

            @Override
            public void onNext(String item) {
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onComplete() {
            }
         });
         mycache.listen(new CacheListenerOptions().clustered(), CacheEntryEventType.CREATED).subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
            }

            @Override
            public void onNext(CacheEntryEvent<String, String> event) {
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {
            }
         });
      }
   }
}
