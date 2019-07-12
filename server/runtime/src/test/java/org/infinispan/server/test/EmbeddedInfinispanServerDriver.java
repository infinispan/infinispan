package org.infinispan.server.test;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.management.MBeanServerConnection;

import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.DefaultExitHandler;
import org.infinispan.server.Server;
import org.infinispan.test.TestingUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EmbeddedInfinispanServerDriver extends InfinispanServerDriver {
   public static final int OFFSET_FACTOR = 100;
   List<Server> servers;
   List<CompletableFuture<Integer>> serverFutures;

   protected EmbeddedInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(configuration, InetAddress.getLoopbackAddress());
   }

   @Override
   protected void start(String name, File rootDir, String configurationFile) {
      servers = new ArrayList<>();
      serverFutures = new ArrayList<>();
      for (int i = 0; i < configuration.numServers(); i++) {
         File serverRoot = createServerHierarchy(rootDir, Integer.toString(i));
         Properties properties = new Properties();
         properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, new File(rootDir, Server.DEFAULT_SERVER_CONFIG).getAbsolutePath());
         properties.setProperty(Server.INFINISPAN_PORT_OFFSET, Integer.toString(i * OFFSET_FACTOR));
         properties.setProperty(Server.INFINISPAN_CLUSTER_NAME, name);
         properties.setProperty(TEST_HOST_ADDRESS, testHostAddress.getHostName());
         Server server = new Server(serverRoot, new File(configurationFile), properties);
         server.setExitHandler(new DefaultExitHandler());
         serverFutures.add(server.run());
         servers.add(server);
      }
      // Ensure that the cluster has formed
      List<DefaultCacheManager> cacheManagers = servers.stream().map(server -> server.getCacheManagers().values().iterator().next()).collect(Collectors.toList());
      TestingUtil.blockUntilViewsReceived(30_000, cacheManagers.toArray(new CacheContainer[]{}));
   }

   @Override
   protected void stop() {
      RuntimeException aggregate = new RuntimeException();
      for (int i = 0; i < servers.size(); i++) {
         Server server = servers.get(i);
         server.getExitHandler().exit(0);
         try {
            serverFutures.get(i).get();
         } catch (Throwable t) {
            aggregate.addSuppressed(t);
         }
      }
      if (aggregate.getSuppressed().length > 0) {
         throw aggregate;
      }
   }

   @Override
   public InetSocketAddress getServerAddress(int server, int port) {
      return new InetSocketAddress("localhost", port + server * OFFSET_FACTOR);
   }

   @Override
   public void pause(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void resume(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void stop(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void kill(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public MBeanServerConnection getJmxConnection(int server) {
      DefaultCacheManager cacheManager = servers.get(server).getCacheManagers().values().iterator().next();
      return cacheManager.getCacheManagerConfiguration().globalJmxStatistics().mbeanServerLookup().getMBeanServer();
   }
}
