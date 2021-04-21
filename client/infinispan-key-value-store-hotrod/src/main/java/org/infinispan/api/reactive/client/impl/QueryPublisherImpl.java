package org.infinispan.api.reactive.client.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.infinispan.query.dsl.Query;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Flowable;

/**
 * Implements {@link Publisher<T>} interface for client/server mode.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class QueryPublisherImpl<T> implements Publisher<T> {
   private final ExecutorService executorService;
   private final Query query;

   public QueryPublisherImpl(Query query, ExecutorService executorService) {
      this.query = query;
      this.executorService = executorService;
   }

   @Override
   public void subscribe(Subscriber<? super T> subscriber) {
      CompletableFuture.supplyAsync(() ->
            (List<T>) query.execute().list(), executorService)
            .whenComplete((r, ex) -> {
               Flowable<T> flowable;
               if (ex != null) {
                  flowable = Flowable.empty();
                  flowable.subscribe(subscriber);
                  subscriber.onError(ex.getCause());
               } else {
                  flowable = Flowable.fromIterable(r);
                  flowable.subscribe(subscriber);
               }
            });
   }
}
