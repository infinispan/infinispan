package org.infinispan.api.search.reactive;

import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.collections.reactive.KeyValueEntry;
import org.reactivestreams.Publisher;

public interface ContinuousQueryPublisher<K, V> extends Publisher<KeyValueEntry<K, V>>, Flow.Publisher<KeyValueEntry<K, V>> {

   ContinuousQueryPublisher<K, V> query(String ickleQuery);

   ContinuousQueryPublisher<K, V> query(String ickleQuery, QueryParameters params);

   ContinuousQueryPublisher<K, V> withTimeout(long timeout, TimeUnit timeUnit);
}
