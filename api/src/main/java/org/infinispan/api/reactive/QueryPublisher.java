package org.infinispan.api.reactive;

import org.reactivestreams.Publisher;

public interface QueryPublisher<T> extends Publisher<T> {

   QueryPublisher<T> withQueryParameter(String name, Object value);

   QueryPublisher<T> withQueryParameters(QueryParameters params);
}
