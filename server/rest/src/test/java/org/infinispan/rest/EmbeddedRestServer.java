package org.infinispan.rest;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.embedded.netty4.NettyRestServer;

public class EmbeddedRestServer {

   final NettyRestServer server;
   final EmbeddedCacheManager cacheManager;
   final RestServerConfiguration configuration;

   public EmbeddedRestServer(EmbeddedCacheManager cacheManager, RestServerConfiguration configuration) {
      this.cacheManager = cacheManager;
      this.configuration = configuration;
      this.server = NettyRestServer.createServer(configuration, cacheManager);
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
