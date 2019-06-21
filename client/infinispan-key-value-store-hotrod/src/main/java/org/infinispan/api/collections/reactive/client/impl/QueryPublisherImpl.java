package org.infinispan.api.collections.reactive.client.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.infinispan.api.reactive.QueryPublisher;
import org.infinispan.query.dsl.Query;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;

/**
 * Implements {@link QueryPublisher} interface for client/server mode.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class QueryPublisherImpl<T> implements QueryPublisher<T> {
   private final ExecutorService executorService;
   private final Query query;

   public QueryPublisherImpl(Query query, ExecutorService executorService) {
      this.query = query;
      this.executorService = executorService;
   }

   @Override
   public void subscribe(Subscriber<? super T> subscriber) {
      CompletableFuture.supplyAsync(() ->
            (List<T>) query.list())
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
