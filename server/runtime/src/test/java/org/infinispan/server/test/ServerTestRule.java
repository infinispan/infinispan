package org.infinispan.server.test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.time.DefaultTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Util;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.server.Server;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedServerConfiguration;
import org.infinispan.server.DefaultExitHandler;
import org.infinispan.server.RestClient;
import org.infinispan.test.TestingUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import net.spy.memcached.MemcachedClient;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerTestRule implements TestRule {
   List<Server> servers;
   List<CompletableFuture<Integer>> serverFutures;
   int numServers = 2;
   TimeService timeService = DefaultTimeService.INSTANCE;
   RemoteCacheManager hotRodClient;
   RestClient httpClient;
   MemcachedClient memcachedClient;

   @Override
   public Statement apply(Statement base, Description description) {
      return new Statement() {
         @Override
         public void evaluate() throws Throwable {
            String serverBaseName = description.getClassName();
            ServerTestConfiguration annotation = description.getTestClass().getAnnotation(ServerTestConfiguration.class);
            String configurationFile = "clustered.xml";
            if (annotation != null) {
               configurationFile = annotation.configurationFile();
               numServers = annotation.numServers();
            }
            before(serverBaseName, description.getTestClass().getClassLoader().getResource(configurationFile));
            try {
               base.evaluate();
            } finally {
               after();
            }
         }
      };
   }

   private void before(String serverBaseName, URL resource) {
      servers = new ArrayList<>();
      serverFutures = new ArrayList<>();
      for (int i = 0; i < numServers; i++) {
         long start = timeService.time();
         String serverName = serverBaseName + "#" + i;
         File serverRoot = createServerHierarchy(serverName);
         String path = resource.getPath();
         Properties properties = new Properties();
         properties.setProperty(Server.INFINISPAN_PORT_OFFSET, Integer.toString(i * 100));
         Server server = new Server(serverRoot, new File(path), properties);
         server.setExitHandler(new DefaultExitHandler());
         serverFutures.add(server.run());
         servers.add(server);
      }

   }

   private void after() {
      if (hotRodClient != null) {
         hotRodClient.stop();
      }
      for (int i = 0; i < servers.size(); i++) {
         long start = timeService.time();
         Server server = servers.get(i);
         server.getExitHandler().exit(0);
         try {
            serverFutures.get(i).get();
         } catch (Throwable t) {
            throw new RuntimeException(t);
         }
         // delete server root
         Util.recursiveFileRemove(server.getServerRoot());
      }
   }


   private static File createServerHierarchy(String name) {
      File tmp = new File(TestingUtil.tmpDirectory(name));
      for (String dir : Arrays.asList("conf", "data", "log", "lib")) {
         new File(tmp, dir).mkdirs();
      }
      return tmp;
   }

   public RemoteCacheManager hotRodClient() {
      if (hotRodClient == null) {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         for (Server server : servers) {
            for (ProtocolServer ps : server.getProtocolServers().values()) {
               if (ps instanceof HotRodServer) {
                  HotRodServerConfiguration serverConfiguration = ((HotRodServer) ps).getConfiguration();
                  builder.addServer().host(serverConfiguration.publicHost()).port(serverConfiguration.publicPort());
               }
            }
         }
         hotRodClient = new RemoteCacheManager(builder.build());
      }
      return hotRodClient;
   }

   public RestClient restClient() {
      if (httpClient == null) {
         for (Server server : servers) {
            for (ProtocolServer ps : server.getProtocolServers().values()) {
               if (ps instanceof RestServer) {
                  RestServerConfiguration serverConfiguration = ((RestServer) ps).getConfiguration();
                  String baseURI = String.format("%s://%s:%d/%s",
                        serverConfiguration.ssl().enabled() ? "https" : "http",
                        serverConfiguration.host(),
                        serverConfiguration.port(),
                        serverConfiguration.contextPath()
                  );
                  httpClient = new RestClient(baseURI);
               }
            }
         }
      }
      return httpClient;
   }

   public MemcachedClient memcachedClient() {
      if (memcachedClient == null) {
         List<InetSocketAddress> addresses = new ArrayList<>();
         for (Server server : servers) {
            for (ProtocolServer ps : server.getProtocolServers().values()) {
               if (ps instanceof MemcachedServer) {
                  MemcachedServerConfiguration serverConfiguration = ((MemcachedServer) ps).getConfiguration();
                  addresses.add(new InetSocketAddress(serverConfiguration.host(), serverConfiguration.port()));
               }
            }
         }
         try {
            memcachedClient = new MemcachedClient(addresses);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
      return memcachedClient;
   }
}
