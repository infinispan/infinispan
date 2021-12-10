package org.infinispan.server.test.core;

import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_EMBEDDED_TIMEOUT_SECONDS;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.management.MBeanServerConnection;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.DefaultExitHandler;
import org.infinispan.server.ExitStatus;
import org.infinispan.server.Server;
import org.infinispan.test.TestingUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EmbeddedInfinispanServerDriver extends AbstractInfinispanServerDriver {
   public static final int OFFSET_FACTOR = 100;
   private static final int TIMEOUT_SECONDS = Integer.getInteger(INFINISPAN_TEST_SERVER_EMBEDDED_TIMEOUT_SECONDS, 30);
   List<Server> servers;
   List<CompletableFuture<ExitStatus>> serverFutures;

   protected EmbeddedInfinispanServerDriver(InfinispanServerTestConfiguration configuration) {
      super(configuration, InetAddress.getLoopbackAddress());
      System.setProperty("infinispan.security.elytron.skipnonceshutdown", "true");
   }

   protected int clusterPortOffset() {
      if (configuration.site() != null)
         configuration.sitePortOffset();

      return configuration.site() == null ? configuration.getPortOffset() : configuration.sitePortOffset();
   }

   @Override
   protected void start(String name, File rootDir, File configurationFile) {
      servers = new ArrayList<>();
      serverFutures = new ArrayList<>();
      for (int i = 0; i < configuration.numServers(); i++) {
         File serverRoot = createServerHierarchy(rootDir, Integer.toString(i));
         Properties properties = new Properties();
         properties.setProperty(Server.INFINISPAN_SERVER_HOME_PATH, serverRoot.getAbsolutePath());
         properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, new File(rootDir, Server.DEFAULT_SERVER_CONFIG).getAbsolutePath());
         properties.setProperty(Server.INFINISPAN_PORT_OFFSET, Integer.toString(clusterPortOffset() + i * OFFSET_FACTOR));
         properties.setProperty(Server.INFINISPAN_CLUSTER_NAME, name);
         properties.setProperty(Server.INFINISPAN_CLUSTER_STACK, System.getProperty(Server.INFINISPAN_CLUSTER_STACK));
         properties.setProperty(TEST_HOST_ADDRESS, testHostAddress.getHostName());
         properties.setProperty(Server.INFINISPAN_LOG4J_SHUTDOWN, "false");

         configureSite(properties);
         configuration.properties().forEach((k, v) -> properties.put(k, StringPropertyReplacer.replaceProperties((String) v, properties)));
         Server server = new Server(serverRoot, new File(configurationFile.getName()), properties);
         server.setExitHandler(new DefaultExitHandler());
         serverFutures.add(server.run());
         servers.add(server);
      }
      // Ensure that the cluster has formed if we start more than one server
      List<DefaultCacheManager> cacheManagers = servers.stream().map(server -> server.getCacheManagers().values().iterator().next()).collect(Collectors.toList());
      if(cacheManagers.size() > 1) {
         TestingUtil.blockUntilViewsReceived(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS), cacheManagers.toArray(new CacheContainer[]{}));
      }
   }

   @Override
   protected void stop() {
      RuntimeException aggregate = new RuntimeException();
      if (servers != null) {
         for (int i = 0; i < servers.size(); i++) {
            Server server = servers.get(i);
            server.getExitHandler().exit(ExitStatus.SERVER_SHUTDOWN);
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
   }

   @Override
   public InetSocketAddress getServerSocket(int server, int port) {
      return new InetSocketAddress(getServerAddress(server), port + clusterPortOffset() + server * OFFSET_FACTOR);
   }

   @Override
   public InetAddress getServerAddress(int server) {
      return Exceptions.unchecked(() -> InetAddress.getByName("localhost"));
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
   public void restart(int server) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void restartCluster() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isRunning(int server) {
      return servers.get(server).getStatus().allowInvocations();
   }

   @Override
   public MBeanServerConnection getJmxConnection(int server) {
      DefaultCacheManager cacheManager = servers.get(server).getCacheManagers().values().iterator().next();
      return cacheManager.getCacheManagerConfiguration().jmx().mbeanServerLookup().getMBeanServer();
   }

   @Override
   public int getTimeout() {
      return TIMEOUT_SECONDS;
   }

   private void configureSite(Properties properties) {
      if (configuration.site() == null) {
         return; //nothing to configure
      }
      //we need to replace the properties in xsite-stacks.xml
      properties.setProperty("relay.site_name", configuration.site());
      properties.setProperty("jgroups.cluster.mcast_port", Integer.toString(configuration.siteDiscoveryPort()));
      properties.setProperty("jgroups.tcp.port", Integer.toString(7800 + clusterPortOffset()));

   }
}
