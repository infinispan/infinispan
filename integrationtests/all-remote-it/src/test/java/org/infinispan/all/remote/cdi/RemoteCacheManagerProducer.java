package org.infinispan.all.remote.cdi;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;

import org.infinispan.cdi.Remote;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;

public class RemoteCacheManagerProducer {

   @Produces
   @Remote
   @ApplicationScoped
   public static RemoteCacheManager getRemoteCacheManager() {
      return cacheManagerWithMarshaller();
   }
   
   @Produces
   @DefaultCacheWithMarshaller
   @ApplicationScoped
   public static RemoteCache<String, String> getCacheManagerWithMarshaller() {
      return cacheManagerWithMarshaller().getCache();
   }
   
   public void stopRemoteCacheManager(@Disposes @Remote RemoteCacheManager rcm) {                                                                                                                                                                 
      rcm.stop();                                                                                                                                                                                             
   }
   
   private static RemoteCacheManager cacheManagerWithMarshaller() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer()
            .host("127.0.0.1")
            .port(11222)
            .protocolVersion(ConfigurationProperties.DEFAULT_PROTOCOL_VERSION)
            .marshaller(new ProtoStreamMarshaller());
      return new RemoteCacheManager(builder.build());
   }
}
