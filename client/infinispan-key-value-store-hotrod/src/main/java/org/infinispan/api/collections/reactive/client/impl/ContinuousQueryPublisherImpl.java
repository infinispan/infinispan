package org.infinispan.api.collections.reactive.client.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.collections.reactive.CreatedKeyValueEntry;
import org.infinispan.api.collections.reactive.KeyValueEntry;
import org.infinispan.api.collections.reactive.RemovedKeyValueEntry;
import org.infinispan.api.collections.reactive.UpdatedKeyValueEntry;
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

/**
 * Implements the {@link ContinuousQueryPublisher} using RXJava and Flowable API.
 *
 * @author Katia Aresti
 * @since 10.0
 */
public class ContinuousQueryPublisherImpl<K, V> implements ContinuousQueryPublisher<K, V> {

   private final ContinuousQuery<K, V> continuousQuery;
   private final QueryFactory queryFactory;
   private Query query;
   private long timeout;
   private TimeUnit timeUnit;
   private boolean all = true;
   private boolean created;
   private boolean updated;
   private boolean removed;
   private ContinuousQueryListener listener;
   private Flowable flowable;
   private Set<Subscriber<? super KeyValueEntry<K, V>>> subscribers = ConcurrentHashMap.newKeySet();

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
   public ContinuousQueryPublisher<K, V> withQueryParameter(String name, Object value) {
      query.setParameter(name, value);
      return this;
   }

   @Override
   public ContinuousQueryPublisher<K, V> withQueryParameters(QueryParameters params) {
      query.setParameters(params.asMap());
      return this;
   }

   @Override
   public ContinuousQueryPublisher<K, V> created() {
      all = false;
      created = true;
      return this;
   }

   @Override
   public ContinuousQueryPublisher<K, V> removed() {
      all = false;
      removed = true;
      return this;
   }

   @Override
   public ContinuousQueryPublisher<K, V> updated() {
      all = false;
      updated = true;
      return this;
   }

   @Override
   public ContinuousQueryPublisher<K, V> withTimeout(long timeout, TimeUnit timeUnit) {
      this.timeout = timeout;
      this.timeUnit = timeUnit;
      return this;
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
      subscribers.add(subscriber);
      if (flowable == null) {
         flowable = createContinuousQueryFlowable();
      }
      flowable.subscribe(subscriber);
   }

   private Flowable createContinuousQueryFlowable() {
      return Flowable.create(e -> {
         this.listener = createContinuousQueryListener(e);
         continuousQuery.addContinuousQueryListener(query, listener);
      }, BackpressureStrategy.BUFFER);
   }

   @Override
   public void dispose() {
      continuousQuery.removeContinuousQueryListener(listener);
      subscribers.forEach(Subscriber::onComplete);
      subscribers.clear();
      listener = null;
      flowable = null;
   }

   private ContinuousQueryListener createContinuousQueryListener(FlowableEmitter<Object> e) {
      return new ContinuousQueryListener<K, V>() {
         @Override
         public void resultJoining(K key, V value) {
            if (all || created)
               e.onNext(new CreatedKeyValueEntry<>(key, value));
         }

         @Override
         public void resultUpdated(K key, V value) {
            if (all || updated)
               e.onNext(new UpdatedKeyValueEntry<>(key, value));
         }

         @Override
         public void resultLeaving(K key) {
            if (all || removed)
               e.onNext(new RemovedKeyValueEntry<>(key, null));
         }
      };
   }
}
