package org.infinispan.server.test.api;

import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;

/**
 *  REST operations for the testing framework
 *
 * @author Tristan Tarrant
 * @since 10
 */
public class HotRodTestClientDriver extends BaseTestClientDriver<HotRodTestClientDriver> {
   private final TestServer testServer;
   private final TestClient testClient;
   private ConfigurationBuilder clientConfiguration;
   private int port = 11222;

   public HotRodTestClientDriver(TestServer testServer, TestClient testClient,
                                 Consumer<ConfigurationBuilder> additionalConfigurations) {
      this.testServer = testServer;
      this.testClient = testClient;

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(1).connectionPool().maxActive(1);
      additionalConfigurations.accept(builder);
      applyDefaultConfiguration(builder);
      this.clientConfiguration = builder;
   }

   /**
    * Provide a custom client configuration to connect to the server.
    *
    * @param clientConfiguration
    * @return the current {@link HotRodTestClientDriver} instance with the client configuration override
    */
   public HotRodTestClientDriver withClientConfiguration(ConfigurationBuilder clientConfiguration) {
      this.clientConfiguration = applyDefaultConfiguration(clientConfiguration);
      return this;
   }

   /**
    * The {@link Marshaller} to be used by the client.
    *
    * @param marshallerClass
    * @return the current {@link HotRodTestClientDriver} instance with the Marshaller configuration override
    */
   public HotRodTestClientDriver withMarshaller(Class<? extends Marshaller> marshallerClass) {
      this.clientConfiguration.marshaller(marshallerClass);
      return this;
   }


   public HotRodTestClientDriver withPort(int port) {
      this.port = port;
      return this;
   }

   /**
    * Gets a cache with the name of the method where this method is being called from
    *
    * @return {@link RemoteCache}, the cache is such exist
    */
   public <K, V> RemoteCache<K, V> get() {
      RemoteCacheManager remoteCacheManager = testClient.registerResource(testServer.newHotRodClient(clientConfiguration));
      String name = testClient.getMethodName(qualifier);
      return remoteCacheManager.getCache(name);
   }

   /**
    * Created a cache with the name of the method where this method is being called from.
    * If the cache already exists, retrieves the existing cache
    *
    * @return {@link RemoteCache}, the cache
    */
   public <K, V> RemoteCache<K, V> create() {
      return create(-1);
   }

   /**
    * Create a cache adding in the initial server list the server address given by the index
    * @param index the server index, -1 for all
    * @return {@link RemoteCache}, the cache
    */
   public <K, V> RemoteCache<K, V> create(int index) {
      RemoteCacheManager remoteCacheManager;
      if (index >= 0) {
         remoteCacheManager = createRemoteCacheManager(index);
      } else {
         remoteCacheManager = createRemoteCacheManager();
      }
      String name = testClient.getMethodName(qualifier);
      if (serverConfiguration != null) {
         return remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, serverConfiguration);
      } else if (mode != null) {
         return remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, "org.infinispan." + mode.name());
      } else {
         return remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, "org.infinispan." + CacheMode.DIST_SYNC.name());
      }
   }

   public RemoteCacheManager createRemoteCacheManager() {
      return testClient.registerResource(testServer.newHotRodClient(clientConfiguration, port));
   }

   public RemoteCacheManager createRemoteCacheManager(int index) {
      return testClient.registerResource(testServer.newHotRodClient(clientConfiguration, port, index));
   }

   @Override
   public HotRodTestClientDriver self() {
      return this;
   }

   private ConfigurationBuilder applyDefaultConfiguration(ConfigurationBuilder builder) {
      if (testServer.isContainerRunWithDefaultServerConfig()) {
         // Configure admin user by default
         builder.security().authentication().username(TestUser.ADMIN.getUser()).password(TestUser.ADMIN.getPassword());
      }
      return builder;
   }
}
