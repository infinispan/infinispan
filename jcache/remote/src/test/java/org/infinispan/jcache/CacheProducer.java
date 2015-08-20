package org.infinispan.jcache;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

public class CacheProducer {

   @Produces
   @ApplicationScoped
   public RemoteCacheManager createCacheManager() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.addServer().host("127.0.0.1").port(JCacheTwoCachesAnnotationsTest.hotRodServer1.getPort());
      return new RemoteCacheManager(cb.build());
   }
}