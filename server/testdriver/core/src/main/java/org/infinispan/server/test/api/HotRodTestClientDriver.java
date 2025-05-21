package org.infinispan.server.test.api;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.core.TestClient;
import org.infinispan.server.test.core.TestServer;
import org.infinispan.server.test.core.TestSystemPropertyNames;

/**
 * REST operations for the testing framework
 *
 * @author Tristan Tarrant
 * @since 10
 */
public class HotRodTestClientDriver extends BaseTestClientDriver<HotRodTestClientDriver> {
   private final TestServer testServer;
   private final TestClient testClient;
   private ConfigurationBuilder clientConfiguration;
   private int port = 11222;

   public HotRodTestClientDriver(TestServer testServer, TestClient testClient) {
      this.testServer = testServer;
      this.testClient = testClient;

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(1).connectionPool().maxActive(1);
      applyDefaultConfiguration(builder);
      this.clientConfiguration = builder;
   }

   /**
    * Provide a custom client configuration to connect to the server.
    *
    * @param clientConfiguration
    * @return the current {@link HotRodTestClientDriver} instance with the client configuration
    *         override
    */
   public HotRodTestClientDriver withClientConfiguration(ConfigurationBuilder clientConfiguration) {
      this.clientConfiguration = applyDefaultConfiguration(clientConfiguration);
      return this;
   }

   /**
    * Provide the Client Intelligence override
    *
    * @param clientIntelligence
    * @return the current {@link HotRodTestClientDriver} instance with the client intelligence
    *         override
    */
   public HotRodTestClientDriver withClientConfiguration(ClientIntelligence clientIntelligence) {
      clientConfiguration.clientIntelligence(clientIntelligence);
      return this;
   }

   /**
    * The {@link Marshaller} to be used by the client.
    *
    * @param marshallerClass
    * @return the current {@link HotRodTestClientDriver} instance with the Marshaller configuration
    *         override
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
      RemoteCacheManager remoteCacheManager = createRemoteCacheManager();
      String name = testClient.getMethodName(qualifiers);
      return remoteCacheManager.getCache(name);
   }

   /**
    * Gets a cache with the provided name
    *
    * @return {@link RemoteCache}, the cache is such exist
    */
   public <K, V> RemoteCache<K, V> get(String name) {
      RemoteCacheManager remoteCacheManager = createRemoteCacheManager();
      return remoteCacheManager.getCache(name);
   }

   /**
    * Created a cache with the name of the method where this method is being called from. If the
    * cache already exists, retrieves the existing cache
    *
    * @return {@link RemoteCache}, the cache
    */
   public <K, V> RemoteCache<K, V> create() {
      return create(-1);
   }

   public static BasicConfiguration toConfiguration(String template) {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.valueOf(template));
      builder.statistics().enable();
      return builder.build();
   }

   /**
    * Create a cache adding in the initial server list the server address given by the index
    *
    * @param index
    *           the server index, -1 for all
    * @return {@link RemoteCache}, the cache
    */
   public <K, V> RemoteCache<K, V> create(int index) {
      RemoteCacheManager remoteCacheManager;
      if (index >= 0) {
         remoteCacheManager = createRemoteCacheManager(index);
      } else {
         remoteCacheManager = createRemoteCacheManager();
      }
      String name = testClient.getMethodName(qualifiers);
      if (serverConfiguration != null) {
         return remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, serverConfiguration);
      }
      if (Boolean.getBoolean(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_NEWER_THAN_14)) {
         // Newer servers doesn't support templates, so we use the cache configuration directly
         return mode != null
               ? remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, toConfiguration(mode))
               : remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name,
                     toConfiguration("DIST_SYNC"));
      }
         return mode != null
               ? remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, "org.infinispan." + mode)
               : remoteCacheManager.administration().withFlags(flags).getOrCreateCache(name, "org.infinispan.DIST_SYNC");
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
