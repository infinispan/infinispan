package org.infinispan.api.collections.reactive.client.impl;

import org.infinispan.api.search.reactive.ReactiveContinuousQuery;
import org.infinispan.api.search.reactive.ReactiveQuery;
import org.infinispan.api.search.reactive.SearchableStore;
import org.infinispan.client.hotrod.RemoteCache;

/**
 * @since 10.0
 */
public class SearchableKeyValueStoreImpl<K, V> extends KeyValueStoreImpl<K, V> implements SearchableStore<V> {

   public SearchableKeyValueStoreImpl(RemoteCache<K, V> cache, RemoteCache<K, V> cacheReturnValues) {
      super(cache, cacheReturnValues);
   }

   @Override
   public ReactiveQuery find(String ickleQuery) {
      throw new UnsupportedOperationException("TBD");
   }

   @Override
   public ReactiveContinuousQuery findContinuously(String ickleQuery) {
      throw new UnsupportedOperationException("TBD");
   }
}
