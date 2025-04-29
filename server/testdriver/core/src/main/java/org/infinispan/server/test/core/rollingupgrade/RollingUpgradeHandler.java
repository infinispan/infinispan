package org.infinispan.server.test.core.rollingupgrade;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.Server;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;

public class RollingUpgradeHandler {
   private final RollingUpgradeConfiguration configuration;
   private final Consumer<String> logConsumer;

   private String toImageCreated;
   private String fromImageCreated;

   private ContainerInfinispanServerDriver fromDriver;
   private ContainerInfinispanServerDriver toDriver;

   private RemoteCacheManager remoteCacheManager;

   private STATE currentState = STATE.NOT_STARTED;

   public enum STATE {
      /**
       * The upgrade procedure has not been started yet
       */
      NOT_STARTED,
      /**
       * This state signals that all the old version nodes are running currently
       */
      OLD_RUNNING,
      /**
       * This state signals that one old node has been shut down. The remaining nodes may be a mixture of old and/or new
       */
      REMOVED_OLD,
      /**
       * This state signals that a new node was just added. The remaining nodes may be a mixture of old and/or new
       */
      ADDED_NEW,
      /**
       * This state signals that the upgrade completed and all old nodes have been removed and all new nodes are running
       */
      NEW_RUNNING,
      /**
       * The upgrade procedure encountered some type of error
       */
      ERROR
   }

   private RollingUpgradeHandler(RollingUpgradeConfiguration configuration) {
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

   public RollingUpgradeConfiguration getConfiguration() {
      return configuration;
   }

   public RemoteCacheManager getRemoteCacheManager() {
      return remoteCacheManager;
   }

   public STATE getCurrentState() {
      return currentState;
   }

   public static void performUpgrade(RollingUpgradeConfiguration configuration) throws InterruptedException {
      String site1Name = "site1";
      RollingUpgradeHandler handler = new RollingUpgradeHandler(configuration);

      int nodeCount = configuration.nodeCount();
      String versionFrom = configuration.fromVersion();
      String versionTo = configuration.toVersion();
      try {
         handler.logConsumer.accept("Starting " + nodeCount + " node to version " + versionFrom);
         handler.fromDriver = handler.startNode(false, configuration.nodeCount(), configuration.nodeCount(),
               site1Name, configuration.jgroupsProtocol(), null);
         handler.currentState = STATE.NEW_RUNNING;

         try (RemoteCacheManager manager = handler.createRemoteCacheManager()) {
            handler.remoteCacheManager = manager;
            configuration.initialHandler().accept(handler);

            for (int i = 0; i < nodeCount; ++i) {
               handler.logConsumer.accept("Shutting down 1 node from version: " + versionFrom);
               int nodeId = nodeCount - i - 1;
               String volumeId = handler.fromDriver.volumeId(nodeId);
               handler.fromDriver.stop(nodeId);

               handler.currentState = STATE.REMOVED_OLD;

               if (!ensureServersWorking(handler, nodeCount - 1)) {
                  handler.logConsumer.accept("Servers are: " + Arrays.toString(manager.getServers()));
                  throw new IllegalStateException("Servers did not shut down properly within 30 seconds, assuming error");
               }

               handler.logConsumer.accept("Starting 1 node to version " + versionTo);
               if (handler.toDriver == null) {
                  handler.toDriver = handler.startNode(true, 1, nodeCount, site1Name,
                        configuration.jgroupsProtocol(), volumeId);
               } else {
                  handler.toDriver.startAdditionalServer(nodeCount, volumeId);
               }

               handler.currentState = STATE.ADDED_NEW;

               if (!ensureServersWorking(handler, nodeCount)) {
                  handler.logConsumer.accept("Servers are only: " + Arrays.toString(manager.getServers()));
                  throw new IllegalStateException("Servers did not cluster within 30 seconds, assuming error");
               }
            }

            handler.currentState = STATE.NEW_RUNNING;
         }
      } catch (Throwable t) {
         handler.currentState = STATE.ERROR;
         configuration.exceptionHandler().accept(t, handler);
         throw t;
      } finally {
         handler.cleanup();
      }
   }

   private static boolean ensureServersWorking(RollingUpgradeHandler handler, int expectedCount) throws InterruptedException {
      long begin = System.nanoTime();
      while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - begin) < handler.configuration.serverCheckTimeSecs()) {
         handler.logConsumer.accept("Checking to ensure cluster formed properly, expecting " + expectedCount + " servers");
         if (handler.configuration.isValidServerState().test(handler)) {
            return true;
         }
         Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      }
      handler.logConsumer.accept("Cluster state check timed out after " + handler.configuration.serverCheckTimeSecs() +
            " secs, check for possible issues");
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

   public RemoteCacheManager createRemoteCacheManager() {
      TestUser user = TestUser.ADMIN;

      String hotrodURI = "hotrod://" + user.getUser() + ":" + user.getPassword() + "@" + fromDriver.getServerAddress(0).getHostAddress() + ":11222";
      logConsumer.accept("Creating RCM with uri: " + hotrodURI);

      return new RemoteCacheManager(hotrodURI);
   }

   private ContainerInfinispanServerDriver startNode(boolean toOrFrom, int nodeCount, int expectedCount, String clusterName,
                                                            String protocol, String volumeId) {
      ServerConfigBuilder builder = new ServerConfigBuilder("infinispan.xml", true);
      builder.runMode(ServerRunMode.CONTAINER);
      builder.numServers(nodeCount);
      builder.expectedServers(expectedCount);
      builder.clusterName(clusterName);
      builder.property(Server.INFINISPAN_CLUSTER_STACK, protocol);
      builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_REQUIRE_JOIN_TIMEOUT, "true");
      if (configuration.useSharedDataMount()) {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true");
      }

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
            imageName = toImageCreated = versionToUse.toLowerCase() + "-to";
         } else {
            assert fromImageCreated == null;
            imageName = fromImageCreated = versionToUse.toLowerCase() + "-from";
         }
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_SNAPSHOT_IMAGE_NAME, imageName);
      } else {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_VERSION, versionToUse);
      }
      InfinispanServerTestConfiguration config = builder.createServerTestConfiguration();

      ContainerInfinispanServerDriver driver = (ContainerInfinispanServerDriver) ServerRunMode.CONTAINER.newDriver(config);
      driver.prepare(versionToUse);
      if (volumeId != null) {
         if (nodeCount != 1) {
            throw new IllegalArgumentException("nodeCount " + nodeCount + " must be 1 when a volumeId is passed " + volumeId);
         }
         driver.configureImage(versionToUse);
         driver.startAdditionalServer(expectedCount, volumeId);
      } else {
         driver.start(versionToUse);
      }
      return driver;
   }
}
