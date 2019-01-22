package org.infinispan.api.search.reactive;

import org.infinispan.api.collections.reactive.Query;
import org.reactivestreams.Publisher;

public interface Searchable<V> {
   Publisher<V> find(String ickleQuery);

   Publisher<V> find(Query query);

   Publisher<V> findContinuous(String ickleQuery);

   Publisher<V> findContinuous(Query query);
}
