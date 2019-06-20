package org.infinispan.api.collections.reactive.client.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import org.infinispan.api.reactive.QueryParameters;
import org.infinispan.api.reactive.QueryPublisher;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;

/**
 * Implements {@link QueryPublisher} interface for client/server mode.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class QueryPublisherImpl<T> implements QueryPublisher<T> {

   private final QueryFactory queryFactory;
   private ExecutorService executorService;
   private Query query;

   public QueryPublisherImpl(QueryFactory queryFactory, ExecutorService executorService, String ickleQuery) {
      this.queryFactory = queryFactory;
      this.executorService = executorService;
      query = queryFactory.create(ickleQuery);
   }

   @Override
   public QueryPublisher<T> withQueryParameter(String name, Object value) {
      query.setParameter(name, value);
      return this;
   }

   @Override
   public QueryPublisher<T> withQueryParameters(QueryParameters params) {
      query.setParameters(params.asMap());
      return this;
   }

   @Override
   public void subscribe(Subscriber<? super T> subscriber) {
      queryPublisherSubscriber(subscriber);
   }

   private void queryPublisherSubscriber(Subscriber<? super T> rsSubscriber) {
      CompletableFuture.supplyAsync(() ->
                  (List<T>) query.list()
            , executorService).thenAccept(r -> {
         Flowable<T> flowable = Flowable.fromIterable(r);
         flowable.subscribe(rsSubscriber);
      });
   }
}
