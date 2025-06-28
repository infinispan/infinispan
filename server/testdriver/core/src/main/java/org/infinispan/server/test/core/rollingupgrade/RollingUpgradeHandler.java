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
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.util.OS;
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
import org.jboss.shrinkwrap.api.spec.JavaArchive;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

public class RollingUpgradeHandler {
   record ConfigAndDriver(InfinispanServerTestConfiguration infinispanServerTestConfiguration, ContainerInfinispanServerDriver containerInfinispanServerDriver) {}

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final AtomicInteger clusterOffset = new AtomicInteger();

   private final RollingUpgradeConfiguration configuration;

   private final int nodeCount;
   private final String clusterName;
   // Holds a value of how many nodes are left to upgrade
   private int nodeLeft;

   private final String versionFrom;
   private final String versionTo;

   private String toImageCreated;
   private String fromImageCreated;

   private ConfigAndDriver fromDriver;
   private ConfigAndDriver toDriver;

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
      this.nodeCount = configuration.nodeCount();
      this.nodeLeft = nodeCount;
      this.versionFrom = configuration.fromVersion();
      this.versionTo = configuration.toVersion();
      this.configuration = configuration;
      this.restClients = new RestClient[nodeCount];
      this.memcachedClients = new MemcachedClient[nodeCount];

      this.clusterName = "rolling-upgrade-" + clusterOffset.getAndIncrement();
   }

   public String getToImageCreated() {
      return toImageCreated;
   }

   public String getFromImageCreated() {
      return fromImageCreated;
   }

   public ContainerInfinispanServerDriver getFromDriver() {
      return fromDriver.containerInfinispanServerDriver;
   }

   public ContainerInfinispanServerDriver getToDriver() {
      return toDriver.containerInfinispanServerDriver;
   }

   public InfinispanServerTestConfiguration getFromConfig() {
      return fromDriver.infinispanServerTestConfiguration;
   }

   public InfinispanServerTestConfiguration getToConfig() {
      return toDriver.infinispanServerTestConfiguration;
   }

   public RollingUpgradeConfiguration getConfiguration() {
      return configuration;
   }

   public RemoteCacheManager getRemoteCacheManager() {
      if (remoteCacheManager == null) {
         remoteCacheManager = createRemoteCacheManager();
      }
      return remoteCacheManager;
   }

   public RestClient rest(int server, RestClientConfigurationBuilder builder) {
      if (restClients[server] != null) return restClients[server];

      InetAddress address = getCurrentState() == STATE.OLD_RUNNING
            ? fromDriver.containerInfinispanServerDriver.getServerAddress(server)
            : toDriver.containerInfinispanServerDriver.getServerAddress(server);

      builder.addServer().host(address.getHostAddress()).port(11222);
      return restClients[server] = RestClient.forConfiguration(builder.build());
   }

   public RedisClusterClient resp(RedisURI.Builder builder) {
      if (respClient != null) return respClient;

      int nodeCount = configuration.nodeCount();
      Function<Integer, InetAddress> addressFunction = i ->
            fromDriver.containerInfinispanServerDriver.isRunning(i) ?
                  fromDriver.containerInfinispanServerDriver.getServerAddress(i) :
                  toDriver.containerInfinispanServerDriver.getServerAddress(i);
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
            ? fromDriver.containerInfinispanServerDriver.getServerAddress(server)
            : toDriver.containerInfinispanServerDriver.getServerAddress(server);

      try {
         return memcachedClients[server] = new MemcachedClient(builder.build(), Collections.singletonList(InetSocketAddress.createUnresolved(address.getHostAddress(), 11222)));
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   public STATE getCurrentState() {
      return currentState;
   }

   /**
    * Similar to {@link #performUpgrade(RollingUpgradeConfiguration)} except that it will stop processing the upgrade
    * after it has a mixed cluster of 1 new and (n-1) old nodes. This allows for processing operations during this
    * period. When done you should complete the Future when it can proceed or set it's exception in whic case it will
    * clean up early.
    * @param configuration the configuration to use
    * @return a future to signal when it should complete the upgrade
    */
   public static RollingUpgradeHandler runUntilMixed(RollingUpgradeConfiguration configuration) throws InterruptedException {
      // TODO: eventually support xsite
      RollingUpgradeHandler handler = new RollingUpgradeHandler(configuration);

      try {
         log.debugf("Starting %d nodes to version %s", handler.nodeCount, handler.versionFrom);
         handler.fromDriver = handler.startNode(false, configuration.nodeCount(), configuration.nodeCount(),
               handler.clusterName, configuration.jgroupsProtocol(), null,configuration.serverConfigurationFile(),
               configuration.defaultServerConfigurationFile(), configuration.customArtifacts(),
               configuration.mavenArtifacts(), configuration.properties());
         handler.currentState = STATE.OLD_RUNNING;

         configuration.initialHandler().accept(handler);

         handler.removeOldAndAddNew(handler.clusterName);

         handler.currentState = STATE.NEW_RUNNING;
      } catch (Throwable t) {
         try {
            handler.exceptionEncountered(t);
            throw t;
         } finally {
            handler.cleanup();
         }
      }

      return handler;
   }

   private void removeOldAndAddNew(String site1Name) throws InterruptedException {
      log.debugf("Shutting down 1 node from version: %s", versionFrom);
      int nodeId = nodeLeft-- - 1;
      String volumeId = fromDriver.containerInfinispanServerDriver.volumeId(nodeId);
      fromDriver.containerInfinispanServerDriver.stop(nodeId);
      cleanup(nodeId);

      currentState = STATE.REMOVED_OLD;

      if (!ensureServersWorking(this, nodeCount - 1)) {
         if (log.isDebugEnabled() && remoteCacheManager != null) {
            log.debugf("Servers are: %s", Arrays.toString(remoteCacheManager.getServers()));
         }
         throw new IllegalStateException("Servers did not shut down properly within 30 seconds, assuming error");
      }

      log.debugf("Starting 1 node to version %s", versionTo);
      if (toDriver == null) {
         toDriver = startNode(true, 1, nodeCount, site1Name,
               configuration.jgroupsProtocol(), volumeId, configuration.serverConfigurationFile(),
               configuration.defaultServerConfigurationFile(), configuration.customArtifacts(),
               configuration.mavenArtifacts(), configuration.properties());
      } else {
         toDriver.containerInfinispanServerDriver.startAdditionalServer(nodeCount, volumeId);
      }

      currentState = STATE.ADDED_NEW;

      if (!ensureServersWorking(this, nodeCount)) {
         if (log.isDebugEnabled() && remoteCacheManager != null) {
            log.debugf("Servers are: %s", Arrays.toString(remoteCacheManager.getServers()));
         }
         throw new IllegalStateException("Servers did not cluster within 30 seconds, assuming error");
      }
   }

   public void complete() {
      try {
         while (nodeLeft > 0) {
            removeOldAndAddNew(clusterName);
            currentState = STATE.NEW_RUNNING;
         }
      } catch (Throwable e) {
         exceptionEncountered(e);
      } finally {
         cleanup();
      }
   }

   public void exceptionEncountered(Throwable t) {
      currentState = STATE.ERROR;
      configuration.exceptionHandler().accept(t, this);
   }

   public static void performUpgrade(RollingUpgradeConfiguration configuration) throws InterruptedException {
      RollingUpgradeHandler ruh = runUntilMixed(configuration);
      try {
         ruh.complete();
      } catch (Throwable t) {
         ruh.exceptionEncountered(t);
      } finally {
         ruh.cleanup();
      }
   }

   private static boolean ensureServersWorking(RollingUpgradeHandler handler, int expectedCount) throws InterruptedException {
      long begin = System.nanoTime();
      while (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - begin) < handler.configuration.serverCheckTimeSecs()) {
         log.debugf("Checking to ensure cluster formed properly, expecting %d servers", expectedCount);
         // Allow the caller to delay us by waiting until the stage is complete
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
         fromDriver.containerInfinispanServerDriver.stop(configuration.fromVersion());
      }
      if (toDriver != null) {
         toDriver.containerInfinispanServerDriver.stop(configuration.toVersion());
      }

      Util.close(remoteCacheManager);
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

   private RemoteCacheManager createRemoteCacheManager() {
      assert currentState == STATE.OLD_RUNNING;

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.maxRetries(1).connectionPool().maxActive(1);
      if (OS.getCurrentOs().equals(OS.MAC_OS) || OS.getCurrentOs().equals(OS.WINDOWS)) {
         builder.clientIntelligence(ClientIntelligence.BASIC);
      }

      if (fromDriver.infinispanServerTestConfiguration.isDefaultFile()) {
         TestUser user = TestUser.ADMIN;
         builder.security().authentication().username(user.getUser()).password(user.getPassword());
      }

      // Apply the configuration updates after applying others but not the server info as test shouldn't be updating it
      builder = configuration.configurationHandler().apply(builder);

      for (int i = 0; i < fromDriver.infinispanServerTestConfiguration.numServers(); i++) {
         InetSocketAddress address = fromDriver.containerInfinispanServerDriver.getServerSocket(i, 11222);
         builder.addServer().host(address.getHostString()).port(address.getPort());
      }

      return fromDriver.containerInfinispanServerDriver.createRemoteCacheManager(builder);
   }

   private ConfigAndDriver startNode(boolean toOrFrom, int nodeCount, int expectedCount,
                                                     String clusterName, String protocol, String volumeId,
                                                     String serverConfigurationFile, boolean defaultServerConfigurationFile,
                                                     JavaArchive[] artifacts, String[] mavenArtifacts, Properties properties) {
      ServerConfigBuilder builder = new ServerConfigBuilder(serverConfigurationFile, defaultServerConfigurationFile);
      builder.runMode(ServerRunMode.CONTAINER);
      builder.numServers(nodeCount);
      builder.expectedServers(expectedCount);
      builder.clusterName(clusterName);
      properties.forEach((k, v) -> {
         if (k instanceof String && v instanceof String) {
            builder.property((String) k, (String) v);
         }
      });
      builder.artifacts(artifacts);
      builder.mavenArtifacts(mavenArtifacts);
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
      return new ConfigAndDriver(config, driver);
   }
}
