package org.infinispan.api.search.reactive;

import java.util.concurrent.Flow;

import org.reactivestreams.Publisher;

public interface QueryPublisher<T> extends Publisher<T>, Flow.Publisher<T> {

   QueryPublisher<T> query(String ickleQuery);

   QueryPublisher<T> withQueryParameter(String name, Object value);

   QueryPublisher<T> withQueryParameters(QueryParameters params);

   QueryPublisher<T> limit(int limit);

   QueryPublisher<T> skip(int skip);
}
