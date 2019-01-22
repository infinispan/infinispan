package org.infinispan;

import org.infinispan.api.collections.reactive.ReactiveCache;

public interface Infinispan {

   <K, V> ReactiveCache<K, V> getReactiveCache(String name);

   InfinispanAdmin administration();
}
