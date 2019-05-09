package org.infinispan.server.test;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Util;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.DefaultExitHandler;
import org.infinispan.server.Server;
import org.infinispan.test.TestingUtil;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class EmbeddedServerDriver extends ServerDriver {
   public static final int OFFSET_FACTOR = 100;
   List<Server> servers;
   List<CompletableFuture<Integer>> serverFutures;
   private File confDir;

   protected EmbeddedServerDriver(String name, ServerTestConfiguration configuration) {
      super(name, configuration);
   }

   @Override
   public void before() {
      servers = new ArrayList<>();
      serverFutures = new ArrayList<>();
      String testDir = TestingUtil.tmpDirectory(name);
      Util.recursiveFileRemove(testDir);
      File rootDir = new File(testDir);
      confDir = new File(rootDir, Server.DEFAULT_SERVER_CONFIG);
      confDir.mkdirs();
      createUserFile(confDir, "admin");
      createUserFile(confDir, "public");
      createKeyStores(confDir);
      for (int i = 0; i < configuration.numServers(); i++) {
         File serverRoot = createServerHierarchy(rootDir, Integer.toString(i));
         Properties properties = new Properties();
         properties.setProperty(Server.INFINISPAN_SERVER_CONFIG_PATH, confDir.getAbsolutePath());
         properties.setProperty(Server.INFINISPAN_PORT_OFFSET, Integer.toString(i * OFFSET_FACTOR));
         properties.setProperty(Server.INFINISPAN_CLUSTER_NAME, name);
         Server server = new Server(serverRoot, new File(configuration.configurationFile()), properties);
         server.setExitHandler(new DefaultExitHandler());
         serverFutures.add(server.run());
         servers.add(server);
      }
      // Ensure that the cluster has formed
      List<DefaultCacheManager> cacheManagers = servers.stream().map(server -> server.getCacheManagers().values().iterator().next()).collect(Collectors.toList());
      TestingUtil.blockUntilViewsReceived(10_000, cacheManagers.toArray(new CacheContainer[]{}));
   }

   @Override
   public void after() {
      for (int i = 0; i < servers.size(); i++) {
         Server server = servers.get(i);
         server.getExitHandler().exit(0);
         try {
            serverFutures.get(i).get();
         } catch (Throwable t) {
            throw new RuntimeException(t);
         }
      }
   }

   @Override
   public InetSocketAddress getServerAddress(int server, int port) {
      return InetSocketAddress.createUnresolved("localhost", port + server * OFFSET_FACTOR);
   }
}
