package org.infinispan.rest.profiling;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.rest.server.RestServer;

public class NewServerHandler implements ServerHandler {

   RestServer restServer;

   @Override
   public void start(EmbeddedCacheManager cacheManager) {
      RestServerConfigurationBuilder restServerConfiguration = new RestServerConfigurationBuilder();
      restServerConfiguration.host("localhost").port(8080);
      restServer = new RestServer();
      restServer.start(restServerConfiguration.build(), cacheManager);
   }

   @Override
   public void stop() {
      restServer.stop();
   }
}
