package org.infinispan.server.test.core;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;

/**
 * InfinispanContainer is an easy way to manage an Infinispan Server container.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 * @deprecated since 16.0 use {@link org.infinispan.testcontainers.InfinispanContainer}
 **/
@Deprecated(since = "16.0")
public class InfinispanContainer extends org.infinispan.testcontainers.InfinispanContainer {
   /**
    * Creates a {@link RemoteCacheManager} configured to connect to the containerized server
    * @return
    */
   public RemoteCacheManager getRemoteCacheManager() {
      return getRemoteCacheManager(new ConfigurationBuilder());
   }

   public RemoteCacheManager getRemoteCacheManager(ConfigurationBuilder builder) {
      builder.addServer().host(getHost()).port(getMappedPort(DEFAULT_HOTROD_PORT))
            .security().authentication().username(getEnvMap().get(USER)).password(getEnvMap().get(PASS));
      return new RemoteCacheManager(builder.build());
   }
}
