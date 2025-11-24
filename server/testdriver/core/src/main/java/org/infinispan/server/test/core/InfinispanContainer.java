package org.infinispan.server.test.core;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.testcontainers.utility.DockerImageName;

/**
 * InfinispanContainer is an easy way to manage an Infinispan Server container.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 * @deprecated since 16.0, will be removed in 16.1 use {@link org.infinispan.testcontainers.InfinispanContainer}
 **/
@Deprecated(since = "16.0", forRemoval = true)
public class InfinispanContainer extends org.infinispan.testcontainers.InfinispanContainer {

   public InfinispanContainer(String imageName) {
      super(imageName);
   }

   public InfinispanContainer(final DockerImageName dockerImageName) {
      super(dockerImageName);
   }

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
