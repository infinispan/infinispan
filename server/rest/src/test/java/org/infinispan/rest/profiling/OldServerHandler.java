package org.infinispan.rest.profiling;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.embedded.netty4.NettyRestServer;

public class OldServerHandler implements ServerHandler {

   NettyRestServer server;

   @Override
   public void start(EmbeddedCacheManager cacheManager) {
      RestServerConfigurationBuilder restServerConfiguration = new RestServerConfigurationBuilder();
      restServerConfiguration.host("localhost").port(8080);
      server = NettyRestServer.createServer(restServerConfiguration.build(), cacheManager);
      server.start();
   }

   @Override
   public void stop() {
      server.stop();
   }
}
