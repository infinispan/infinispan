package org.infinispan.api.collections.reactive.client.impl;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.search.reactive.ContinuousQueryPublisher;
import org.infinispan.api.search.reactive.QueryParameters;
import org.infinispan.api.search.reactive.QueryPublisher;
import org.reactivestreams.Subscriber;

public class ContinuousQueryPublisherImpl<T> implements ContinuousQueryPublisher<T> {

   @Override
   public QueryPublisher<T> query(String query) {
      return null;
   }

   @Override
   public QueryPublisher<T> query(String query, QueryParameters params) {
      return null;
   }

   @Override
   public QueryPublisher<T> limit(int limit) {
      return null;
   }

   @Override
   public QueryPublisher<T> skip(int skip) {
      return null;
   }

   @Override
   public QueryPublisher<T> withTimeout(long timeout, TimeUnit timeUnit) {
      return null;
   }

   @Override
   public void subscribe(Subscriber<? super T> subscriber) {

   }
}
