package org.infinispan.server.test.core.rollingupgrade;

import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.Server;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;

public class UpgradeHandler {
   private final UpgradeConfiguration configuration;
   private final Consumer<String> logConsumer;

   private String toImageCreated;
   private String fromImageCreated;

   private ContainerInfinispanServerDriver fromDriver;
   private ContainerInfinispanServerDriver toDriver;

   private UpgradeHandler(UpgradeConfiguration configuration) {
      this.configuration = configuration;
      this.logConsumer = configuration.logConsumer();
   }

   public String getToImageCreated() {
      return toImageCreated;
   }

   public String getFromImageCreated() {
      return fromImageCreated;
   }

   public ContainerInfinispanServerDriver getFromDriver() {
      return fromDriver;
   }

   public ContainerInfinispanServerDriver getToDriver() {
      return toDriver;
   }

   public UpgradeConfiguration getConfiguration() {
      return configuration;
   }

   public static void performUpgrade(UpgradeConfiguration configuration) throws InterruptedException {
      String site1Name = "site1";
      UpgradeHandler handler = new UpgradeHandler(configuration);

      int nodeCount = configuration.nodeCount();
      String versionFrom = configuration.fromVersion();
      String versionTo = configuration.toVersion();
      try {
         handler.logConsumer.accept("Starting " + nodeCount + " node to version " + versionFrom);
         handler.fromDriver = handler.startNode(false, configuration.nodeCount(), configuration.nodeCount(),
               site1Name, configuration.jgroupsProtocol());

         try (RemoteCacheManager manager = handler.createRemoteCacheManager()) {
            RemoteCache<String, String> cache = configuration.initialHandler().apply(manager);

            for (int i = 0; i < nodeCount; ++i) {
               handler.logConsumer.accept("Shutting down 1 node from version: " + versionFrom);
               handler.fromDriver.stop(nodeCount - i - 1);

               if (!handler.ensureServersWorking(cache, nodeCount - 1)) {
                  handler.logConsumer.accept("Servers are: " + Arrays.toString(manager.getServers()));
                  throw new IllegalStateException("Servers did not shut down properly within 30 seconds, assuming error");
               }

               handler.logConsumer.accept("Starting 1 node to version " + versionTo);
               if (handler.toDriver == null) {
                  handler.toDriver = handler.startNode(true, 1, nodeCount, site1Name,
                        configuration.jgroupsProtocol());
               } else {
                  handler.toDriver.startAdditionalServer(nodeCount);
               }

               if (!handler.ensureServersWorking(cache, nodeCount)) {
                  handler.logConsumer.accept("Servers are only: " + Arrays.toString(manager.getServers()));
                  throw new IllegalStateException("Servers did not cluster within 30 seconds, assuming error");
               }
            }
         }
      } catch (Throwable t) {
         configuration.exceptionHandler().accept(t, handler);
         throw t;
      } finally {
         handler.cleanup();
      }
   }

   private boolean ensureServersWorking(RemoteCache<String, String> cache, int expectedCount) throws InterruptedException {
      long begin = System.nanoTime();
      while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - begin) < configuration.serverCheckTimeSecs()) {
         logConsumer.accept("Attempting remote call to ensure cluster formed properly");
         String value = cache.get("foo");
         if (value != null && !value.equals("bar")) {
            throw new IllegalStateException("Remote cache returned " + value + " instead of bar");
         }

         Set<SocketAddress> servers = cache.getCacheTopologyInfo().getSegmentsPerServer().keySet();
         if (servers.size() == expectedCount) {
            logConsumer.accept("Servers are: " + servers);
            return true;
         }
         Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      }
      logConsumer.accept("Improper shutdown detected, servers are: " + cache.getCacheTopologyInfo().getSegmentsPerServer().keySet());
      return false;
   }

   private void cleanup() {
      if (fromDriver != null) {
         fromDriver.stop(configuration.fromVersion());
      }
      if (toDriver != null) {
         toDriver.stop(configuration.toVersion());
      }
   }

   private RemoteCacheManager createRemoteCacheManager() {
      TestUser user = TestUser.ADMIN;

      String hotrodURI = "hotrod://" + user.getUser() + ":" + user.getPassword() + "@" + fromDriver.getServerAddress(0).getHostAddress() + ":11222";
      logConsumer.accept("Creating RCM with uri: " + hotrodURI);

      return new RemoteCacheManager(hotrodURI);
   }

   private ContainerInfinispanServerDriver startNode(boolean toOrFrom, int nodeCount, int expectedCount, String clusterName,
                                                            String protocol) {
      ServerConfigBuilder builder = new ServerConfigBuilder("infinispan.xml", true);
      builder.runMode(ServerRunMode.CONTAINER);
      builder.numServers(nodeCount);
      builder.expectedServers(expectedCount);
      builder.clusterName(clusterName);
      builder.property(Server.INFINISPAN_CLUSTER_STACK, protocol);
      builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_REQUIRE_JOIN_TIMEOUT, "true");

      String versionToUse = toOrFrom ? configuration.toVersion() : configuration.fromVersion();

      if (versionToUse.startsWith("image://")) {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME, versionToUse.substring("image://".length()));
      } else if (versionToUse.startsWith("file://")) {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR, versionToUse.substring("file://".length()));
         // For simplicity trim down to the directory name for the rest of the test
         versionToUse = Path.of(versionToUse).getFileName().toString();

         String imageName;
         if (toOrFrom) {
            assert toImageCreated == null;
            imageName = toImageCreated = ContainerInfinispanServerDriver.SNAPSHOT_IMAGE + "-to";
         } else {
            assert fromImageCreated == null;
            imageName = fromImageCreated = ContainerInfinispanServerDriver.SNAPSHOT_IMAGE + "-from";
         }
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_SNAPSHOT_IMAGE_NAME, imageName);
      } else {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_VERSION, versionToUse);
      }
      InfinispanServerTestConfiguration config = builder.createServerTestConfiguration();

      ContainerInfinispanServerDriver driver = (ContainerInfinispanServerDriver) ServerRunMode.CONTAINER.newDriver(config);
      driver.prepare(versionToUse);
      driver.start(versionToUse);
      return driver;
   }
}
