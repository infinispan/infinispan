package org.infinispan.server.test.util;

import java.util.function.Consumer;

import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.junit.ClassResource;

/**
 * Use with {@link org.junit.ClassRule @ClassRule} to initialize a {@link RemoteCacheManager} in a non-static method
 * and release it after all the methods in the class.
 *
 * @author Dan Berindei
 * @since 10.0
 */
public class ClassRemoteCacheManager extends ClassResource<RemoteCacheManager> {
   public ClassRemoteCacheManager() {
   }

   public ClassRemoteCacheManager(Consumer<RemoteCacheManager> closer) {
      super(closer);
   }

   public RemoteCacheManager cacheRemoteCacheManager(Configuration configuration) throws Exception {
      return cache(() -> new RemoteCacheManager(configuration));
   }

   public RemoteCacheManager cacheRemoteCacheManager(ConfigurationBuilder configurationBuilder) throws Exception {
      return cacheRemoteCacheManager(configurationBuilder.build());
   }
   public RemoteCacheManager cacheRemoteCacheManager(RemoteInfinispanServer server) throws Exception {
      return cacheRemoteCacheManager(ITestUtils.createConfigBuilder(server).build());
   }
   public RemoteCacheManager cacheRemoteCacheManager(RemoteInfinispanServer server, String protocolVersion) throws Exception {
      return cacheRemoteCacheManager(ITestUtils.createConfigBuilder(server, protocolVersion).build());
   }
}
