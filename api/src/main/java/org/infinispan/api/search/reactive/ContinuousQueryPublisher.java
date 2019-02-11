package org.infinispan.api.search.reactive;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;

public interface ContinuousQueryPublisher<T> extends Publisher<T> {

   QueryPublisher<T> query(String query);

   QueryPublisher<T> query(String query, QueryParameters params);

   QueryPublisher<T> limit(int limit);

   QueryPublisher<T> skip(int skip);

   QueryPublisher<T> withTimeout(long timeout, TimeUnit timeUnit);
}
