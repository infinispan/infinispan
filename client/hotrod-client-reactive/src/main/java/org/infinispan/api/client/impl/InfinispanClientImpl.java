package org.infinispan.api.client.impl;

import org.infinispan.api.ClientConfig;
import org.infinispan.api.Infinispan;
import org.infinispan.api.collections.reactive.KeyValueStore;
import org.infinispan.api.collections.reactive.client.impl.KeyValueStoreImpl;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

public class InfinispanClientImpl implements Infinispan {

   private RemoteCacheManager cacheManager;

   public InfinispanClientImpl(ClientConfig clientConfig) {
      if (clientConfig instanceof ClientConfigurationLoader.ConfigurationWrapper) {
         ClientConfigurationLoader.ConfigurationWrapper wrapper = (ClientConfigurationLoader.ConfigurationWrapper) clientConfig;
         cacheManager = new RemoteCacheManager(wrapper.getConfiguration());
      }
   }

   @Override
   public <K, V> KeyValueStore<K, V> getKeyValueStore(String name) {
      RemoteCache<K, V> cache = cacheManager.getCache(name, false);
      RemoteCache<K, V> cacheWithReturnValues = cacheManager.getCache(name, true);
      return new KeyValueStoreImpl(cache, cacheWithReturnValues);
   }

   /* TODO: Remove, visible for test now */
   public void setCacheManager(RemoteCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }
}
