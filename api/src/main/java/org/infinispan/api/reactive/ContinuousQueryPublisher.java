package org.infinispan.api.reactive;

import org.reactivestreams.Publisher;

public interface ContinuousQueryPublisher<K, V> extends Publisher<KeyValueEntry<K, V>> {

   ContinuousQueryPublisher<K, V> query(String ickleQuery);

   ContinuousQueryPublisher<K, V> withQueryParameter(String name, Object value);

   ContinuousQueryPublisher<K, V> withQueryParameters(QueryParameters params);

   ContinuousQueryPublisher<K, V> created();

   ContinuousQueryPublisher<K, V> updated();

   ContinuousQueryPublisher<K, V> removed();
}
