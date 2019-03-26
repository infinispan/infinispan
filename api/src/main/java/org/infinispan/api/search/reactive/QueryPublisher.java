package org.infinispan.api.search.reactive;

import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Publisher;

public interface QueryPublisher<T> extends Publisher<T>, Flow.Publisher<T> {

   QueryPublisher<T> query(String ickleQuery);

   QueryPublisher<T> withQueryParameters(QueryParameters params);

   QueryPublisher<T> withTimeout(long timeout, TimeUnit timeUnit);

   QueryPublisher<T> limit(int limit);

   QueryPublisher<T> skip(int skip);
}
