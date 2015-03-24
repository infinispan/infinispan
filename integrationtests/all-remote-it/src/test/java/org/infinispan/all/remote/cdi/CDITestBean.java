
package org.infinispan.all.remote.cdi;

import javax.inject.Inject;

import org.infinispan.cdi.Remote;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.Configuration;

public class CDITestBean {

   @Inject
   @DefaultCacheWithMarshaller
   private RemoteCache<String, String> cache;

   @Inject
   @Remote
   private RemoteCache<String, String> remoteCache;
   
   public String cacheGet(String key){
      return cache.get(key);
   }
   
   public void cachePut(String key, String value) {
      cache.put(key, value);
   }
   
   public String remoteCacheGet(String key) {
      return remoteCache.get(key);
   }
   
   public Configuration getRemoteCacheManagerConfiguration() {
      return remoteCache.getRemoteCacheManager().getConfiguration();
   }
   
}
