package org.infinispan.api.collections.reactive.client.impl;

import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.search.reactive.QueryParameters;
import org.infinispan.api.search.reactive.QueryPublisher;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;

/**
 *
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class QueryPublisherImpl<T> implements QueryPublisher<T> {

   private final QueryFactory queryFactory;
   private Query query;
   private long timeout;
   private TimeUnit timeUnit;

   public QueryPublisherImpl(QueryFactory queryFactory) {
      this.queryFactory = queryFactory;
   }

   @Override
   public QueryPublisher<T> query(String ickleQuery) {
      query = queryFactory.create(ickleQuery);
      return this;
   }

   @Override
   public QueryPublisher<T> withQueryParameters(QueryParameters params) {
      query.setParameters(params.asMap());
      return this;
   }

   @Override
   public QueryPublisher<T> limit(int limit) {
      query = query.maxResults(limit);
      return this;
   }

   @Override
   public QueryPublisher<T> skip(int skip) {
      query = query.startOffset(skip);
      return this;
   }

   @Override
   public QueryPublisher<T> withTimeout(long timeout, TimeUnit timeUnit) {
      this.timeout = timeout;
      this.timeUnit = timeUnit;
      return this;
   }

   @Override
   public void subscribe(Flow.Subscriber<? super T> subscriber) {
      Subscriber<? super T> rsSubscriber = FlowAdapters.toSubscriber(subscriber);
      queryPublisherSubscriber(rsSubscriber);
   }

   @Override
   public void subscribe(Subscriber<? super T> subscriber) {
      queryPublisherSubscriber(subscriber);
   }

   private void queryPublisherSubscriber(Subscriber<? super T> rsSubscriber) {
      Flowable<T> flowable = Flowable.fromIterable(query.list());
      if (timeout > 0) {
         flowable.take(timeout, timeUnit);
      }
      flowable.subscribe(rsSubscriber);
   }
}
