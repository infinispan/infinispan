package org.infinispan.server.test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.RestClient;
import org.infinispan.server.Server;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import net.spy.memcached.MemcachedClient;

/**
 * Creates a cluster of servers to be used for running multiple tests It performs the following tasks:
 * <ul>
 * <li>It creates a temporary directory using the test name</li>
 * <li>It creates a common configuration directory to be shared by all servers</li>
 * <li>It creates a runtime directory structure for each server in the cluster (data, log, lib)</li>
 * <li>It populates the configuration directory with multiple certificates (ca.pfx, server.pfx, user1.pfx,
 * user2.pfx)</li>
 * <li>It populates the configuration directory with user/group property files</li>
 * </ul>
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerTestRule implements TestRule {
   Map<Integer, RemoteCacheManager> hotRodClients = new LinkedHashMap<>();
   Map<Integer, RestClient> httpClients = new LinkedHashMap<>();
   Map<Integer, MemcachedClient> memcachedClients = new LinkedHashMap<>();

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            ServerTestConfiguration configuration = description.getTestClass().getAnnotation(ServerTestConfiguration.class);
            Objects.requireNonNull(configuration, "The class under test must be annotated with @ServerTestConfiguration");
            ServerDriver serverDriver = configuration.runMode().newDriver(description.getClassName(), configuration);
            serverDriver.before();
            try {
               base.evaluate();
            } finally {
               serverDriver.after();
            }
         }
      };
   }

   void after(ServerTestConfiguration configuration) {
      hotRodClients.values().forEach(rcm -> rcm.stop());
      hotRodClients.clear();
      memcachedClients.values().forEach(c -> c.shutdown());
      memcachedClients.clear();
   }

   /**
    * @return a client configured against the first Hot Rod endpoint exposed by the server
    */
   public RemoteCacheManager hotRodClient() {
      initHotRodClients();
      return hotRodClients.values().iterator().next();
   }

   /**
    * @param port the port of the Hot Rod endpoint for which a client should be returned
    * @return a client configured against the Hot Rod endpoint exposed by the server on the specified port
    */
   public RemoteCacheManager hotRodClient(int port) {
      initHotRodClients();
      return hotRodClients.get(port);
   }

   List<Server> getServers() {
      return null;
   }

   private void initHotRodClients() {
      if (hotRodClients.isEmpty()) {
         // Add all known server addresses
         Map<Integer, ConfigurationBuilder> hotRodConfig = new LinkedHashMap<>();
         String confDir = "";
         List<Server> servers = getServers();
         for (int i = 0; i < servers.size(); i++) {
            int portOffset = i * 100;
            for (ProtocolServer ps : servers.get(i).getProtocolServers().values()) {
               if (ps instanceof HotRodServer) {
                  HotRodServerConfiguration serverConfiguration = ((HotRodServer) ps).getConfiguration();
                  int configuredPort = serverConfiguration.publicPort() - portOffset;
                  ConfigurationBuilder builder = hotRodConfig.computeIfAbsent(configuredPort, port -> new ConfigurationBuilder());
                  builder.addServer().host(serverConfiguration.publicHost()).port(serverConfiguration.publicPort());
                  if (serverConfiguration.ssl().enabled()) {
                     builder.security().ssl().enable().trustStoreFileName(new File(confDir, "server.pfx").getAbsolutePath()).trustStorePassword("secret".toCharArray());
                  }
                  if (serverConfiguration.authentication().enabled()) {
                     builder.security()
                           .authentication()
                           .enable();
                  }
               }
            }
         }
         hotRodClients.putAll(hotRodConfig.entrySet().stream().collect(
               Collectors.toMap(
                     e -> e.getKey(),
                     e -> new RemoteCacheManager(e.getValue().build())
               )
         ));
      }
   }

   /**
    * @return a client configured against the first REST endpoint exposed by the server
    */
   public RestClient restClient() {
      initRestClients();
      return httpClients.values().iterator().next();
   }

   /**
    * @param port the port of the REST endpoint for which a client should be returned
    * @return a client configured against the REST endpoint exposed by the server on the specified port
    */
   public RestClient restClient(int port) {
      initRestClients();
      return httpClients.get(port);
   }

   private void initRestClients() {
      if (httpClients.isEmpty()) {
         for (Server server : getServers()) {
            for (ProtocolServer ps : server.getProtocolServers().values()) {
               if (ps instanceof RestServer) {
                  RestServerConfiguration serverConfiguration = ((RestServer) ps).getConfiguration();
                  String baseURI = String.format("%s://%s:%d/%s",
                        serverConfiguration.ssl().enabled() ? "https" : "http",
                        serverConfiguration.host(),
                        serverConfiguration.port(),
                        serverConfiguration.contextPath()
                  );
                  httpClients.put(serverConfiguration.port(), new RestClient(baseURI));
               }
            }
         }
      }
   }

   /**
    * @return a client configured against the first Memcached endpoint exposed by the server
    */
   public MemcachedClient memcachedClient() {
      initHotRodClients();
      return memcachedClients.values().iterator().next();
   }

   /**
    * @param port the port of the Memcached endpoint for which a client should be returned
    * @return a client configured against the Memcached endpoint exposed by the server on the specified port
    */
   public MemcachedClient memcachedClient(int port) {
      initMemcachedClient();
      return memcachedClients.get(port);
   }

   private void initMemcachedClient() {
      if (memcachedClients.isEmpty()) {
         Map<Integer, List<InetSocketAddress>> addresses = new LinkedHashMap<>();
         for (Server server : getServers()) {
            for (ProtocolServer ps : server.getProtocolServers().values()) {
               if (ps instanceof MemcachedServer) {
                  MemcachedServerConfiguration serverConfiguration = ((MemcachedServer) ps).getConfiguration();
                  List<InetSocketAddress> serverAddresses = addresses.computeIfAbsent(serverConfiguration.port(), port -> new ArrayList<>());
                  serverAddresses.add(new InetSocketAddress(serverConfiguration.host(), serverConfiguration.port()));
               }
            }
         }

         memcachedClients.putAll(addresses.entrySet().stream().collect(
               Collectors.toMap(
                     e -> e.getKey(),
                     e -> newMemcachedClient(e.getValue())
               )
         ));
      }
   }

   private MemcachedClient newMemcachedClient(List<InetSocketAddress> addrs) {
      try {
         return new MemcachedClient(addrs);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }
}
