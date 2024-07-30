package org.infinispan.commons.api.query.impl;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

public class QueryPublisher<E> implements Publisher<E> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final Query<E> query;
   private final int maxBatchSize;

   public QueryPublisher(Query<E> query, int maxBatchSize) {
      this.query = query;
      this.maxBatchSize = maxBatchSize;
   }

   @Override
   public void subscribe(Subscriber<? super E> subscriber) {
      new QuerySubscription<>(maxBatchSize, query)
            .startPublisher().subscribe(subscriber);
   }
}
