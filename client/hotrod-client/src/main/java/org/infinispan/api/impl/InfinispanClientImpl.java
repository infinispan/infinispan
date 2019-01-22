package org.infinispan.api.impl;

import org.infinispan.ClientConfig;
import org.infinispan.Infinispan;
import org.infinispan.InfinispanAdmin;
import org.infinispan.api.collections.reactive.ReactiveCache;
import org.infinispan.api.collections.reactive.client.impl.ReactiveCacheImpl;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;

public class InfinispanClientImpl implements Infinispan {

   private RemoteCacheManager cacheManager;

   public InfinispanClientImpl(ClientConfig clientConfig) {
      cacheManager = new RemoteCacheManager((Configuration) clientConfig);
   }

   @Override
   public <K, V> ReactiveCache<K, V> getReactiveCache(String name) {
      RemoteCache<K, V> cache = cacheManager.getCache(name, false);
      RemoteCache<K, V> cacheWithReturnValues = cacheManager.getCache(name, true);
      return new ReactiveCacheImpl(cache, cacheWithReturnValues);
   }

   @Override
   public InfinispanAdmin administration() {
      return null;
   }

   /* TODO: Remove, visible for test now */
   public void setCacheManager(RemoteCacheManager cacheManager){
      this.cacheManager = cacheManager;
   }
}
