package org.infinispan.api.search.reactive;

import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;

public interface QueryPublisher<T> extends Publisher<T> {

   QueryPublisher<T> query(String ickleQuery);

   QueryPublisher<T> query(String ickleQuery, QueryParameters params);

   QueryPublisher<T> limit(int limit);

   QueryPublisher<T> skip(int skip);

   QueryPublisher<T> withTimeout(long timeout, TimeUnit timeUnit);
}
