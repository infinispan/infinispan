package org.infinispan.server.test.core.rollingupgrade;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.util.Util;
import org.infinispan.server.Server;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

public class RollingUpgradeHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final RollingUpgradeConfiguration configuration;

   private String toImageCreated;
   private String fromImageCreated;

   private ContainerInfinispanServerDriver fromDriver;
   private ContainerInfinispanServerDriver toDriver;

   private RemoteCacheManager remoteCacheManager;
   private RedisClusterClient respClient;
   private MemcachedClient[] memcachedClients;
   private RestClient[] restClients;

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

   public RestClient rest(int server, RestClientConfigurationBuilder builder) {
      if (restClients[server] != null) return restClients[server];

      InetAddress address = getCurrentState() == STATE.OLD_RUNNING
            ? fromDriver.getServerAddress(server)
            : toDriver.getServerAddress(server);

      builder.addServer().host(address.getHostAddress()).port(11222);
      return restClients[server] = RestClient.forConfiguration(builder.build());
   }

   public RedisClusterClient resp(RedisURI.Builder builder) {
      if (respClient != null) return respClient;

      int nodeCount = configuration.nodeCount();
      Function<Integer, InetAddress> addressFunction = i ->
            fromDriver.isRunning(i) ? fromDriver.getServerAddress(i) : toDriver.getServerAddress(i);
      List<RedisURI> uris = new ArrayList<>();
      for (int i = 0; i < nodeCount; i++) {
         InetAddress address = addressFunction.apply(i);
         RedisURI uri = builder
               .withHost(address.getHostAddress())
               .withPort(11222)
               .withTimeout(Duration.ofSeconds(30))
               .build();
         uris.add(uri);
      }
      return respClient = RedisClusterClient.create(uris);
   }

   public MemcachedClient memcached(int server, ConnectionFactoryBuilder builder) {
      if (memcachedClients[server] != null) return memcachedClients[server];

      InetAddress address = getCurrentState() == STATE.OLD_RUNNING
            ? fromDriver.getServerAddress(server)
            : toDriver.getServerAddress(server);

      try {
         return memcachedClients[server] = new MemcachedClient(builder.build(), Collections.singletonList(InetSocketAddress.createUnresolved(address.getHostAddress(), 11222)));
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
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
         log.debugf("Starting %d node to version %s", nodeCount, versionFrom);
         handler.fromDriver = handler.startNode(false, configuration.nodeCount(), configuration.nodeCount(),
               site1Name, configuration.jgroupsProtocol(), null);
         handler.currentState = STATE.OLD_RUNNING;

         try (RemoteCacheManager manager = handler.createRemoteCacheManager()) {
            handler.remoteCacheManager = manager;
            handler.restClients = new RestClient[nodeCount];
            handler.memcachedClients = new MemcachedClient[nodeCount];
            configuration.initialHandler().accept(handler);

            for (int i = 0; i < nodeCount; ++i) {
               log.debugf("Shutting down 1 node from version: %s", versionFrom);
               int nodeId = nodeCount - i - 1;
               String volumeId = handler.fromDriver.volumeId(nodeId);
               handler.fromDriver.stop(nodeId);
               handler.cleanup(i);

               handler.currentState = STATE.REMOVED_OLD;

               if (!ensureServersWorking(handler, nodeCount - 1)) {
                  if (log.isDebugEnabled()) {
                     log.debugf("Servers are: %s", Arrays.toString(manager.getServers()));
                  }
                  throw new IllegalStateException("Servers did not shut down properly within 30 seconds, assuming error");
               }

               log.debugf("Starting 1 node to version %s", versionTo);
               if (handler.toDriver == null) {
                  handler.toDriver = handler.startNode(true, 1, nodeCount, site1Name,
                        configuration.jgroupsProtocol(), volumeId);
               } else {
                  handler.toDriver.startAdditionalServer(nodeCount, volumeId);
               }

               handler.currentState = STATE.ADDED_NEW;

               if (!ensureServersWorking(handler, nodeCount)) {
                  if (log.isDebugEnabled()) {
                     log.debugf("Servers are only: %s", Arrays.toString(manager.getServers()));
                  }
                  throw new IllegalStateException("Servers did not cluster within 30 seconds, assuming error");
               }
            }

            handler.currentState = STATE.NEW_RUNNING;
         }
      } catch (Throwable t) {
         handler.currentState = STATE.ERROR;
         handler.configuration.exceptionHandler().accept(t, handler);
         throw t;
      } finally {
         handler.cleanup();
      }
   }

   private static boolean ensureServersWorking(RollingUpgradeHandler handler, int expectedCount) throws InterruptedException {
      long begin = System.nanoTime();
      while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - begin) < handler.configuration.serverCheckTimeSecs()) {
         log.debugf("Checking to ensure cluster formed properly, expecting %d servers", expectedCount);
         if (handler.configuration.isValidServerState().test(handler)) {
            return true;
         }
         Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      }
      log.debugf("Cluster state check timed out after %d secs, check for possible issues", handler.configuration.serverCheckTimeSecs());
      return false;
   }

   private void cleanup() {
      if (fromDriver != null) {
         fromDriver.stop(configuration.fromVersion());
      }
      if (toDriver != null) {
         toDriver.stop(configuration.toVersion());
      }

      Arrays.stream(restClients).forEach(Util::close);
      Arrays.stream(memcachedClients).forEach(c -> {
         if (c != null) c.shutdown();
      });
      Util.close(respClient);
   }

   private void cleanup(int server) {
      Util.close(restClients[server]);
      restClients[server] = null;

      if (memcachedClients[server] != null) {
         memcachedClients[server].shutdown();
         memcachedClients[server] = null;
      }
   }

   public RemoteCacheManager createRemoteCacheManager() {
      TestUser user = TestUser.ADMIN;

      String hotrodURI = "hotrod://" + user.getUser() + ":" + user.getPassword() + "@" + fromDriver.getServerAddress(0).getHostAddress() + ":11222";
      log.debugf("Creating RCM with uri: %s", hotrodURI);

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
      // If the nodeCount was the same as expected it means it is the start of a fresh cluster. In that case we don't
      // need the join timeout as there shouldn't be any existing nodes in the cluster and don't wait
      builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_REQUIRE_JOIN_TIMEOUT, Boolean.toString(nodeCount != expectedCount));
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
