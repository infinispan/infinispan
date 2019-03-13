package org.infinispan.api.collections.reactive.client.impl;

import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.collections.reactive.KeyValueEntry;
import org.infinispan.api.search.reactive.ContinuousQueryPublisher;
import org.infinispan.api.search.reactive.QueryParameters;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.reactivestreams.FlowAdapters;
import org.reactivestreams.Subscriber;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;

public class ContinuousQueryPublisherImpl<K, V> implements ContinuousQueryPublisher<K, V> {

   private final ContinuousQuery<K, V> continuousQuery;
   private final QueryFactory queryFactory;
   private Query query;

   public ContinuousQueryPublisherImpl(ContinuousQuery<K, V> continuousQuery, QueryFactory queryFactory) {
      this.continuousQuery = continuousQuery;
      this.queryFactory = queryFactory;
   }

   @Override
   public ContinuousQueryPublisher<K, V> query(String ickleQuery) {
      query = queryFactory.create(ickleQuery);
      return this;
   }

   @Override
   public ContinuousQueryPublisher<K, V> query(String ickleQuery, QueryParameters params) {
      query = queryFactory.create(ickleQuery);
      query.setParameters(params.asMap());
      return this;
   }

   @Override
   public ContinuousQueryPublisher<K, V> withTimeout(long timeout, TimeUnit timeUnit) {
      return this;
   }

   @Override
   public void subscribe(Flow.Subscriber<? super KeyValueEntry<K, V>> subscriber) {
      Flowable flowable = createContinuousQueryFlowable();
      flowable.subscribe(FlowAdapters.toSubscriber(subscriber));
   }

   @Override
   public void subscribe(Subscriber<? super KeyValueEntry<K, V>> subscriber) {
      Flowable flowable = createContinuousQueryFlowable();
      flowable.subscribe(subscriber);
   }

   private Flowable createContinuousQueryFlowable() {
      continuousQuery.removeAllListeners();
      return Flowable.create(e -> {
         ContinuousQueryListener listener = new ContinuousQueryListener<K, V>() {
            @Override
            public void resultJoining(K key, V value) {
               e.onNext(new KeyValueEntry<>(key, value));
            }
         };
         continuousQuery.addContinuousQueryListener(query, listener);
      }, BackpressureStrategy.DROP);
   }
}
