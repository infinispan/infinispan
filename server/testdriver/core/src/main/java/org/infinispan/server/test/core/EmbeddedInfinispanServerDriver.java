package org.infinispan.server.test.core;

import static org.infinispan.server.test.core.TestSystemPropertyNames.INFINISPAN_TEST_SERVER_EMBEDDED_TIMEOUT_SECONDS;

import java.io.Closeable;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.management.MBeanServerConnection;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.StringPropertyReplacer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.DefaultExitHandler;
import org.infinispan.server.ExitStatus;
import org.infinispan.server.Server;

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
      System.setProperty(Server.INFINISPAN_ELYTRON_NONCE_SHUTDOWN, "false");
   }

   protected int clusterPortOffset() {
      return configuration.site() == null ? configuration.getPortOffset() : configuration.sitePortOffset();
   }

   @Override
   protected void start(String name, File rootDir, File configurationFile) {
      if ((configuration.archives() != null && configuration.archives().length > 0) || (configuration.mavenArtifacts() != null && configuration.mavenArtifacts().length > 0)) {
         throw new IllegalArgumentException("EmbeddedInfinispanServerDriver doesn't support server artifacts.");
      }
      servers = new ArrayList<>();
      serverFutures = new ArrayList<>();
      for (int i = 0; i < configuration.numServers(); i++) {
         File serverRoot = createServerHierarchy(rootDir, Integer.toString(i));
         copyArtifactsToDataDir();
         Server server = createServerInstance(name, rootDir, configurationFile, i, serverRoot);
         serverFutures.add(server.run());
         servers.add(server);
      }
      // Ensure that the cluster has formed if we start more than one server
      List<EmbeddedCacheManager> cacheManagers = servers.stream().map(Server::getCacheManager).collect(Collectors.toList());
      if(cacheManagers.size() > 1) {
         blockUntilViewsReceived(cacheManagers);
      }
   }

   private Server createServerInstance(String name, File rootDir, File configurationFile, int serverIndex, File serverRoot) {
      Properties properties = new Properties();
      properties.setProperty(Server.INFINISPAN_SERVER_HOME_PATH, serverRoot.getAbsolutePath());
      properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, new File(rootDir, Server.DEFAULT_SERVER_CONFIG).getAbsolutePath());
      properties.setProperty(Server.INFINISPAN_PORT_OFFSET, Integer.toString(clusterPortOffset() + serverIndex * OFFSET_FACTOR));
      properties.setProperty(Server.INFINISPAN_CLUSTER_NAME, name);
      properties.setProperty(Server.INFINISPAN_CLUSTER_STACK, System.getProperty(Server.INFINISPAN_CLUSTER_STACK));
      properties.setProperty(TEST_HOST_ADDRESS, testHostAddress.getHostName());
      properties.setProperty(Server.INFINISPAN_LOG4J_SHUTDOWN, "false");
      if (serverIndex == 0 && configuration.site() == null) {
         properties.setProperty(JOIN_TIMEOUT, "0");
      }
      configureSite(properties);
      configuration.properties().forEach((k, v) -> {
         String value = StringPropertyReplacer.replaceProperties((String) v, properties);
         properties.put(k, value);
         System.setProperty(k.toString(), value);
      });
      Server server = new Server(serverRoot, new File(configurationFile.getName()), properties);
      server.setExitHandler(new DefaultExitHandler());
      return server;
   }

   @Override
   protected void stop() {
      RuntimeException aggregate = new RuntimeException();
      if (servers != null) {
         for (int i = 0; i < servers.size(); i++) {
            try {
               stop(i);
            } catch (Throwable e) {
               aggregate.addSuppressed(e.getCause());
            }
         }
         if (aggregate.getSuppressed().length > 0) {
            throw aggregate;
         }
      }
   }

   @Override
   public InetSocketAddress getServerSocket(int serverIndex, int port) {
      return new InetSocketAddress("127.0.0.1", port + clusterPortOffset() + serverIndex * OFFSET_FACTOR);
   }

   @Override
   public InetAddress getServerAddress(int serverIndex) {
      return Exceptions.unchecked(() -> InetAddress.getByName("127.0.0.1"));
   }

   @Override
   public void pause(int serverIndex) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void resume(int serverIndex) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void stop(int serverIndex) {
      try {
         Server server = servers.get(serverIndex);
         if (server == null)
            return;
         server.getExitHandler().exit(ExitStatus.SERVER_SHUTDOWN);
         serverFutures.get(serverIndex).get();
         serverFutures.set(serverIndex, null);
         servers.set(serverIndex, null);
      } catch (Throwable t) {
         throw new RuntimeException("Cannot stop the server: " + serverIndex, t);
      }
   }

   @Override
   public void kill(int serverIndex) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void restart(int serverIndex) {
      assert !isRunning(serverIndex);
      File serverRoot = serverRoot(getRootDir(), Integer.toString(serverIndex));
      Server server = createServerInstance(getName(), getRootDir(), new File(configuration.configurationFile()),
                                           serverIndex, serverRoot);
      servers.set(serverIndex, server);
      serverFutures.set(serverIndex, server.run());
   }

   @Override
   public void restartCluster() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isRunning(int serverIndex) {
      Server server = servers.get(serverIndex);
      return server != null && server.getStatus().allowInvocations();
   }

   @Override
   public MBeanServerConnection getJmxConnection(int server, String username, String password, Consumer<Closeable> reaper) {
      DefaultCacheManager cacheManager = servers.get(server).getCacheManager();
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

   private static void blockUntilViewsReceived(List<EmbeddedCacheManager> cacheManagers) {
      long failTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS);

      while (System.currentTimeMillis() < failTime) {
         if (areCacheViewsComplete(cacheManagers, false)) {
            return;
         }
         try {
            //noinspection BusyWait
            Thread.sleep(100);
         } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
         }
      }
      //throws exception when "fail=true"
      areCacheViewsComplete(cacheManagers, true);
   }

   public static boolean areCacheViewsComplete(List<EmbeddedCacheManager> cacheManagers, boolean fail) {
      final int memberCount = cacheManagers.size();

      for (EmbeddedCacheManager manager : cacheManagers) {
         if (!isCacheViewComplete(manager.getMembers(), manager.getAddress(), memberCount, fail)) {
            return false;
         }
      }
      return true;
   }

   private static boolean isCacheViewComplete(List<Address> members, Address address, int memberCount, boolean fail) {
      if (members == null || memberCount > members.size()) {
         if (fail) {
            if (members == null) {
               throw new IllegalStateException("Member " + address + " is not connected yet!");
            } else {
               failMissingMembers(members, address, memberCount);
            }
         }
         return false;
      } else if (memberCount < members.size()) {
         failMissingMembers(members, address, memberCount);
      }
      return true;
   }

   private static void failMissingMembers(List<Address> members, Address address, int memberCount) {
      // This is an exceptional condition
      StringBuilder sb = new StringBuilder("Cache at address ");
      sb.append(address);
      sb.append(" had ");
      sb.append(members.size());
      sb.append(" members; expecting ");
      sb.append(memberCount);
      sb.append(". Members were (");
      for (int j = 0; j < members.size(); j++) {
         if (j > 0) {
            sb.append(", ");
         }
         sb.append(members.get(j));
      }
      sb.append(')');

      throw new IllegalStateException(sb.toString());
   }

   @Override
   public String syncFilesFromServer(int server, String dir) {
      //NO-OP
      return getRootDir().toPath().resolve(Integer.toString(server)).toString();
   }

   @Override
   public String syncFilesToServer(int server, String path) {
      //NO-OP
      return path;
   }
}
