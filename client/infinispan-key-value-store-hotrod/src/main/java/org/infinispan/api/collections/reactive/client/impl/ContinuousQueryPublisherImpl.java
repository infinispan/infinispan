package org.infinispan.api.collections.reactive.client.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.api.reactive.ContinuousQueryPublisher;
import org.infinispan.api.reactive.CreatedKeyValueEntry;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.api.reactive.QueryParameters;
import org.infinispan.api.reactive.RemovedKeyValueEntry;
import org.infinispan.api.reactive.UpdatedKeyValueEntry;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.UnicastProcessor;

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
   private boolean all = true;
   private boolean created;
   private boolean updated;
   private boolean removed;
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

   private FlowableProcessor createContinuousQueryFlowable() {
      UnicastProcessor unicastProcessor = UnicastProcessor.create();
      ContinuousQueryListener continuousQueryListener = createContinuousQueryListener(unicastProcessor);
      continuousQuery.addContinuousQueryListener(query, continuousQueryListener);
      return unicastProcessor;
   }

   private ContinuousQueryListener createContinuousQueryListener(UnicastProcessor processor) {
      return new ContinuousQueryListener<K, V>() {
         @Override
         public void resultJoining(K key, V value) {
            if (all || created)
               processor.onNext(new CreatedKeyValueEntry<>(key, value));
         }

         @Override
         public void resultUpdated(K key, V value) {
            if (all || updated)
               processor.onNext(new UpdatedKeyValueEntry<>(key, value));
         }

         @Override
         public void resultLeaving(K key) {
            if (all || removed)
               processor.onNext(new RemovedKeyValueEntry<>(key, null));
         }
      };
   }
}
