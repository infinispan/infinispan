package org.infinispan.api.collections.reactive.client.impl;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.api.reactive.ContinuousQueryPublisher;
import org.infinispan.api.reactive.CreatedKeyValueEntry;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.api.reactive.DeletedKeyValueEntry;
import org.infinispan.api.reactive.UpdatedKeyValueEntry;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.UnicastProcessor;

/**
 * Implements the {@link ContinuousQueryPublisher} using RXJava and Flowable API.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class ContinuousQueryPublisherImpl<K, V> implements ContinuousQueryPublisher<K, V> {

   private final ContinuousQuery<K, V> continuousQuery;
   private final boolean created;
   private final boolean updated;
   private final boolean deleted;
   private final Query query;
   private Flowable flowable;
   private Set<Subscriber<? super KeyValueEntry<K, V>>> subscribers = ConcurrentHashMap.newKeySet();

   public ContinuousQueryPublisherImpl(Query query, ContinuousQuery<K, V> continuousQuery, boolean created, boolean updated, boolean deleted) {
      this.query = query;
      this.continuousQuery = continuousQuery;
      this.created = created;
      this.updated = updated;
      this.deleted = deleted;
   }

   @Override
   public void subscribe(Subscriber<? super KeyValueEntry<K, V>> subscriber) {
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
            if (created)
               processor.onNext(new CreatedKeyValueEntry<>(key, value));
         }

         @Override
         public void resultUpdated(K key, V value) {
            if (updated)
               processor.onNext(new UpdatedKeyValueEntry<>(key, value));
         }

         @Override
         public void resultLeaving(K key) {
            if (deleted)
               processor.onNext(new DeletedKeyValueEntry<>(key, null));
         }
      };
   }
}
