package org.infinispan.api.reactive;

import org.reactivestreams.Publisher;

public interface ContinuousQueryPublisher<K, Z> extends Publisher<KeyValueEntry<K, V>> {

   ContinuousQueryPublisher<K, V> query(String ickleQuery);

   ContinuousQueryPublisher<K, V> query(ContinuousQuery ickleQuery);
}
