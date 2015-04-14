package org.infinispan.rest;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;

public class EmbeddedRestServer {

   final NettyRestServer server;
   final EmbeddedCacheManager cacheManager;
   final RestServerConfiguration configuration;

   public EmbeddedRestServer(EmbeddedCacheManager cacheManager, RestServerConfiguration configuration) {
      this.cacheManager = cacheManager;
      this.configuration = configuration;
      this.server = NettyRestServer.apply(configuration, cacheManager);
   }

   public void start() throws Exception {
      cacheManager.start();
      server.start();
   }

   public void stop() throws Exception {
      server.stop();
      cacheManager.stop();
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

   public String getHost() {
      return configuration.host();
   }

   public int getPort() {
      return configuration.port();
   }
}
