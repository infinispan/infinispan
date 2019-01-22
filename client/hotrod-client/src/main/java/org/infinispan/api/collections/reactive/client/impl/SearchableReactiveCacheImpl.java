package org.infinispan.api.collections.reactive.client.impl;

import org.infinispan.api.collections.reactive.Query;
import org.infinispan.api.search.reactive.Searchable;
import org.infinispan.client.hotrod.RemoteCache;
import org.reactivestreams.Publisher;

/**
 * @since 10.0
 */
public class SearchableReactiveCacheImpl<K, V> extends ReactiveCacheImpl<K, V> implements Searchable<V> {

   public SearchableReactiveCacheImpl(RemoteCache<K, V> cache, RemoteCache<K, V> cacheReturnValues) {
      super(cache, cacheReturnValues);
   }

   @Override
   public Publisher<V> find(String ickleQuery) {
      return null;
   }

   @Override
   public Publisher<V> find(Query query) {
      return null;
   }

   @Override
   public Publisher<V> findContinuous(String ickleQuery) {
      return null;
   }

   @Override
   public Publisher<V> findContinuous(Query query) {
      return null;
   }
}
