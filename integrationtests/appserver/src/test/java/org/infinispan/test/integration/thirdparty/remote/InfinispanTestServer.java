package org.infinispan.test.integration.thirdparty.remote;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.testcontainers.InfinispanContainer;

public class InfinispanTestServer implements AutoCloseable {

   private final InfinispanContainer container;

   public InfinispanTestServer() {
      container = new InfinispanContainer() {
         {
            addFixedExposedPort(DEFAULT_HOTROD_PORT, DEFAULT_HOTROD_PORT);
         }
      };
   }

   public void start() {
      container.start();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(InfinispanContainer.DEFAULT_HOTROD_PORT);
      builder.security().authentication()
            .username(InfinispanContainer.DEFAULT_USERNAME)
            .password(InfinispanContainer.DEFAULT_PASSWORD.toCharArray());
      try (RemoteCacheManager rcm = new RemoteCacheManager(builder.build())) {
         rcm.administration().getOrCreateCache("default", (String) null);
      }
   }

   @Override
   public void close() {
      container.stop();
   }
}
