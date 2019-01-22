package org.infinispan.api.reactive.client.impl;

import org.infinispan.api.reactive.EntryStatus;
import org.infinispan.api.reactive.KeyValueEntry;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.api.continuous.ContinuousQueryListener;
import org.infinispan.query.dsl.Query;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.Flowable;
import io.reactivex.processors.UnicastProcessor;

/**
 * Implements the {@link Publisher<KeyValueEntry<K, V>>} using RXJava and Flowable API.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public class ContinuousQueryPublisherImpl<K, V> implements Publisher<KeyValueEntry<K, V>> {
   private static final Log log = LogFactory.getLog(ContinuousQueryPublisherImpl.class, Log.class);

   private final ContinuousQuery<K, V> continuousQuery;
   private final boolean created;
   private final boolean updated;
   private final boolean deleted;
   private final Query query;
   private Flowable<KeyValueEntry<K, V>> flowable;
   private ContinuousQueryListener<K, V> continuousQueryListener;

   public ContinuousQueryPublisherImpl(Query query, ContinuousQuery<K, V> continuousQuery, boolean created, boolean updated, boolean deleted) {
      this.query = query;
      this.continuousQuery = continuousQuery;
      this.created = created;
      this.updated = updated;
      this.deleted = deleted;
   }

   @Override
   public void subscribe(Subscriber<? super KeyValueEntry<K, V>> subscriber) {
      createContinuousQueryFlowable();
      flowable.subscribe(subscriber);
   }

   synchronized private void createContinuousQueryFlowable() {
      if (flowable == null) {
         UnicastProcessor<KeyValueEntry<K, V>> unicastProcessor = UnicastProcessor.create();
         continuousQueryListener = createContinuousQueryListener(unicastProcessor);
         continuousQuery.addContinuousQueryListener(query, continuousQueryListener);
         unicastProcessor.doOnError(e -> {
            log.error(e);
            continuousQuery.removeContinuousQueryListener(continuousQueryListener);
         }).doOnCancel(() -> continuousQuery.removeContinuousQueryListener(continuousQueryListener));
         flowable = unicastProcessor;
      }
   }

   private ContinuousQueryListener<K, V> createContinuousQueryListener(UnicastProcessor<KeyValueEntry<K, V>> processor) {
      return new ContinuousQueryListener<K, V>() {
         @Override
         public void resultJoining(K key, V value) {
            if (created)
               processor.onNext(new KeyValueEntry<>(key, value, EntryStatus.CREATED));
         }

         @Override
         public void resultUpdated(K key, V value) {
            if (updated)
               processor.onNext(new KeyValueEntry<>(key, value, EntryStatus.UPDATED));
         }

         @Override
         public void resultLeaving(K key) {
            if (deleted)
               processor.onNext(new KeyValueEntry<>(key, null, EntryStatus.DELETED));
         }
      };
   }
}
