package org.infinispan.api.collections.reactive.client.impl;

import java.util.concurrent.ConcurrentHashMap;
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
import io.reactivex.FlowableEmitter;

public class ContinuousQueryPublisherImpl<K, V> implements ContinuousQueryPublisher<K, V> {

   private final ContinuousQuery<K, V> continuousQuery;
   private final QueryFactory queryFactory;
   private Query query;
   private long timeout;
   private TimeUnit timeUnit;

   // Keeps track of the subscribers
   private final ConcurrentHashMap.KeySetView<Subscriber, Boolean> subscribers = ConcurrentHashMap.newKeySet();

   private ContinuousQueryListener listener;
   private Flowable flowable;

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
   public ContinuousQueryPublisher<K, V> withQueryParameters(QueryParameters params) {
      query.setParameters(params.asMap());
      return this;
   }

   @Override
   public ContinuousQueryPublisher<K, V> withTimeout(long timeout, TimeUnit timeUnit) {
      this.timeout = timeout;
      this.timeUnit = timeUnit;
      return this;
   }

   @Override
   public void dispose(Subscriber<KeyValueEntry<K, V>> subscriber) {
      disposeRs(subscriber);
   }

   @Override
   public void dispose(Flow.Subscriber<KeyValueEntry<K, V>> subscriber) {
      disposeRs(FlowAdapters.toSubscriber(subscriber));
   }

   private void disposeRs(Subscriber<KeyValueEntry<K, V>> subscriber) {
      subscribers.remove(subscriber);

      // todo synchronize?
      if (subscribers.isEmpty()) {
         continuousQuery.removeContinuousQueryListener(listener);
         listener = null;
         flowable = null;
      }
   }

   @Override
   public void subscribe(Flow.Subscriber<? super KeyValueEntry<K, V>> subscriber) {
      subscribeRs(FlowAdapters.toSubscriber(subscriber));
   }

   @Override
   public void subscribe(Subscriber<? super KeyValueEntry<K, V>> subscriber) {
      subscribeRs(subscriber);
   }

   private void subscribeRs(Subscriber<? super KeyValueEntry<K, V>> subscriber) {
      // todo:synchronize?
      if (flowable == null) {
         flowable = createContinuousQueryFlowable();
      }
      flowable.subscribe(subscriber);
      subscribers.add(subscriber);
   }

   private Flowable createContinuousQueryFlowable() {
      return Flowable.create(e -> {
         this.listener = createContinuousQueryListener(e);
         continuousQuery.addContinuousQueryListener(query, listener);
      }, BackpressureStrategy.DROP);
   }

   private ContinuousQueryListener createContinuousQueryListener(FlowableEmitter<Object> e) {
      return new ContinuousQueryListener<K, V>() {
         @Override
         public void resultJoining(K key, V value) {
            e.onNext(new KeyValueEntry<>(key, value));
         }
      };
   }
}
