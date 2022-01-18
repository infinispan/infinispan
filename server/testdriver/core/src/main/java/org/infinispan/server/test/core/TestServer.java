package org.infinispan.server.test.core;

import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;

import net.spy.memcached.MemcachedClient;

/**
 * Class that contains all the logic related to the server driver
 *
 * @author Katia Aresti
 * @since 11
 */
public class TestServer {
   protected InfinispanServerDriver serverDriver;
   protected final List<Consumer<File>> configurationEnhancers = new ArrayList<>();
   protected InfinispanServerTestConfiguration configuration;

   public TestServer(InfinispanServerTestConfiguration configuration) {
      this.configuration = configuration;
   }

   public boolean isDriverInitialized() {
      return serverDriver != null;
   }

   public InfinispanServerDriver getDriver() {
      if (serverDriver == null) {
         throw new IllegalStateException("ServerDriver is null. Server driver not initialized");
      }
      return serverDriver;
   }

   public void stopServerDriver(String testName) {
      getDriver().stop(testName);
      serverDriver = null;
   }

   /**
    * @return a client configured against the Hot Rod endpoint exposed by the server
    */
   public RemoteCacheManager newHotRodClient() {
      return newHotRodClient(new ConfigurationBuilder());
   }

   public RemoteCacheManager newHotRodClient(ConfigurationBuilder builder) {
      return newHotRodClient(builder, getDefaultPortNumber());
   }

   /**
    * Create a client adding in the initial server list the server address given by the index.
    * If empty, it will add all servers by default OR those that were configured in the server list
    * @param index the server index
    * @return a client configured against the Hot Rod endpoint exposed by the server
    */
   public RemoteCacheManager newHotRodClient(ConfigurationBuilder builder, int port, int... index) {
      if (index.length == 0) {
         if (builder.servers().isEmpty()) {
            for (int i = 0; i < getDriver().getConfiguration().numServers(); i++) {
               configureHotRodClient(builder, port, i);
            }
         }
      } else {
         for (int i : index) {
            configureHotRodClient(builder, port, i);
         }
      }
      return getDriver().createRemoteCacheManager(builder);
   }

   private void configureHotRodClient(ConfigurationBuilder builder, int port, int i) {
      if (getDriver().getConfiguration().runMode() == ServerRunMode.CONTAINER) {
         ContainerInfinispanServerDriver containerDriver = (ContainerInfinispanServerDriver) serverDriver;
         InfinispanGenericContainer container = containerDriver.getContainer(i);
         builder.addServer().host(container.getIpAddress().getHostAddress()).port(port);
      } else {
         InetSocketAddress serverAddress = getDriver().getServerSocket(i, port);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      }
   }

   public RestClient newRestClient(RestClientConfigurationBuilder builder) {
      return newRestClient(builder, getDefaultPortNumber());
   }

   public RestClient newRestClient(RestClientConfigurationBuilder builder, int port) {
      // Add all known server addresses
      for (int i = 0; i < getDriver().getConfiguration().numServers(); i++) {
         InetSocketAddress serverAddress = getDriver().getServerSocket(i, port);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      }
      return RestClient.forConfiguration(builder.build());
   }

   CloseableMemcachedClient newMemcachedClient() {
      List<InetSocketAddress> addresses = new ArrayList<>();
      for (int i = 0; i < getDriver().getConfiguration().numServers(); i++) {
         InetSocketAddress unresolved = getDriver().getServerSocket(i, 11221);
         addresses.add(new InetSocketAddress(unresolved.getHostName(), unresolved.getPort()));
      }
      MemcachedClient memcachedClient = Exceptions.unchecked(() -> new MemcachedClient(addresses));
      return new CloseableMemcachedClient(memcachedClient);
   }

   public void beforeListeners() {
      configuration.listeners().forEach(l -> l.before(serverDriver));
   }

   public void afterListeners() {
      configuration.listeners().forEach(l -> l.after(serverDriver));
   }

   public boolean isContainerRunWithDefaultServerConfig() {
      return configuration.isDefaultFile() && configuration.runMode() == ServerRunMode.CONTAINER;
   }

   public static class CloseableMemcachedClient implements Closeable {
      final MemcachedClient client;

      public CloseableMemcachedClient(MemcachedClient client) {
         this.client = client;
      }

      public MemcachedClient getClient() {
         return client;
      }

      @Override
      public void close() {
         client.shutdown();
      }
   }

   /**
    * Create a new REST client to connect to the server
    *
    * @param builder client configuration
    * @param n the server number
    *
    * @return a client configured against the nth server
    */
   public RestClient newRestClientForServer(RestClientConfigurationBuilder builder, int port, int n) {
      InetSocketAddress serverAddress = getDriver().getServerSocket(n, port);
      builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
      return RestClient.forConfiguration(builder.build());
   }

   public void add(Consumer<File> enhancer) {
      configurationEnhancers.add(enhancer);
   }

   public void initServerDriver() {
      serverDriver = configuration.runMode().newDriver(configuration);
   }

   public void enhanceConfiguration() {
      configurationEnhancers.forEach(c -> c.accept(serverDriver.getConfDir()));
   }

   public boolean hasCrossSiteEnabled() {
      return configuration.site() != null;
   }

   public String getSiteName() {
      return configuration.site();
   }

   public int getDefaultPortNumber() {
      return 11222;
   }
}
