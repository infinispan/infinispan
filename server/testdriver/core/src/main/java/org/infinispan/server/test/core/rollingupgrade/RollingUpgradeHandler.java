package org.infinispan.server.test.core.rollingupgrade;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.maven.MavenArtifact;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.OS;
import org.infinispan.commons.util.Util;
import org.infinispan.server.Server;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.InfinispanServerListener;
import org.infinispan.server.test.core.InfinispanServerTestConfiguration;
import org.infinispan.server.test.core.ServerConfigBuilder;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.core.Unzip;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.shrinkwrap.api.Archive;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

public class RollingUpgradeHandler {
   public record ConfigAndDriver(InfinispanServerTestConfiguration infinispanServerTestConfiguration,
                                 ContainerInfinispanServerDriver containerInfinispanServerDriver) {
   }

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final RollingUpgradeConfiguration configuration;

   private final int nodeCount;
   private final String clusterName;
   private final String siteName;
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
   private final MemcachedClient[] memcachedClients;
   private final RestClient[] restClients;

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

   private RollingUpgradeHandler(RollingUpgradeConfiguration configuration, String siteName) {
      this.nodeCount = configuration.nodeCount();
      this.nodeLeft = nodeCount;
      this.versionFrom = configuration.fromVersion();
      this.versionTo = configuration.toVersion();
      this.configuration = configuration;
      this.restClients = new RestClient[nodeCount];
      this.memcachedClients = new MemcachedClient[nodeCount];
      this.clusterName = configuration.name();
      this.siteName = siteName;
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
    *
    * @param configuration the configuration to use
    * @return a future to signal when it should complete the upgrade
    */
   public static RollingUpgradeHandler runUntilMixed(RollingUpgradeConfiguration configuration) {
      RollingUpgradeHandler handler = startOldCluster(configuration);

      try {
         handler.upgradeNewNode();
      } catch (Throwable t) {
         try {
            handler.exceptionEncountered(t);
         } finally {
            handler.close();
         }
      }

      return handler;
   }

   public static RollingUpgradeHandler startOldCluster(RollingUpgradeConfiguration configuration) {
      return startOldCluster(configuration, null);
   }

   public static RollingUpgradeHandler startOldCluster(RollingUpgradeConfiguration configuration, String siteName) {
      if (configuration.nodeCount() <= 1) {
         throw new IllegalStateException("Rolling Upgrade requires at least 2 nodes, but received " + configuration.nodeCount());
      }
      RollingUpgradeHandler handler = new RollingUpgradeHandler(configuration, siteName);

      try {
         log.debugf("Starting %d nodes to version %s in cluster %s", handler.nodeCount, handler.versionFrom, handler.clusterName);
         final Archive<?>[] artifacts;
         try {
            if (configuration.customArtifacts().length > 0) {
               // If possible, use the "fromVersion" artifacts
               MavenArtifact mavenArtifacts = new MavenArtifact("org.infinispan", "infinispan-server-tests", configuration.fromVersion(), "artifacts");
               Path artifactsZip = mavenArtifacts.resolveArtifact("zip");
               if (artifactsZip == null) {
                  artifacts = configuration.customArtifacts();
                  log.warnf("Could not download custom artifacts for version %s using %s. Failures may happen", configuration.fromVersion(), mavenArtifacts);
               } else {
                  artifacts = Unzip.unzip(artifactsZip, Paths.get(CommonsTestingUtil.tmpDirectory(), configuration.fromVersion()))
                        .stream().map(p -> ShrinkWrap.createFromZipFile(JavaArchive.class, p.toFile()))
                        .toArray(i -> new Archive<?>[i]);
                  log.infof("Custom artifacts for version %s using %s", configuration.fromVersion(), mavenArtifacts);
               }
            } else {
               artifacts = configuration.customArtifacts();
            }
         } catch (IOException e) {
            throw new UncheckedIOException(e);
         }

         handler.fromDriver = handler.startNode(VersionType.FROM, configuration.nodeCount(), configuration.nodeCount(),
               configuration.jgroupsProtocol(), null, configuration.serverConfigurationFile(),
               configuration.defaultServerConfigurationFile(), artifacts,
               configuration.mavenArtifacts(), configuration.properties(), configuration.listeners());
         handler.currentState = STATE.OLD_RUNNING;

         configuration.initialHandler().accept(handler);
      } catch (Throwable t) {
         try {
            handler.exceptionEncountered(t);
            throw t;
         } finally {
            handler.close();
         }
      }

      return handler;
   }

   public void upgradeNewNode() throws InterruptedException {
      if (nodeLeft <= 0) {
         throw new IllegalStateException("All nodes have already been migrated to a new server. Check invocation count," +
               " configured nodeCount is: " + nodeCount);
      }
      log.debugf("Shutting down 1 node from version: %s for cluster %s", versionFrom, clusterName);
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

      log.debugf("Starting 1 node to version %s at cluster %s", versionTo, clusterName);
      if (toDriver == null) {
         toDriver = startNode(VersionType.TO, 1, nodeCount,
               configuration.jgroupsProtocol(), volumeId, configuration.serverConfigurationFile(),
               configuration.defaultServerConfigurationFile(), configuration.customArtifacts(),
               configuration.mavenArtifacts(), configuration.properties(), configuration.listeners());
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

   public void completeUpgrade(boolean shouldClose) {
      try {
         while (nodeLeft > 0) {
            upgradeNewNode();
            currentState = STATE.NEW_RUNNING;
         }
      } catch (Throwable e) {
         exceptionEncountered(e);
      } finally {
         if (shouldClose) {
            close();
         }
      }
   }

   public void exceptionEncountered(Throwable t) {
      currentState = STATE.ERROR;
      configuration.exceptionHandler().accept(t, this);
   }

   public static void performUpgrade(RollingUpgradeConfiguration configuration) {
      RollingUpgradeHandler ruh = runUntilMixed(configuration);
      try {
         ruh.completeUpgrade(false);
      } catch (Throwable t) {
         ruh.exceptionEncountered(t);
      } finally {
         ruh.close();
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

   public void close() {
      if (fromDriver != null) {
         fromDriver.containerInfinispanServerDriver.stop(configuration.fromVersion());
         fromDriver = null;
      }
      if (toDriver != null) {
         toDriver.containerInfinispanServerDriver.stop(configuration.toVersion());
         toDriver = null;
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

   private ConfigAndDriver startNode(VersionType versionType, int nodeCount, int expectedCount,
                                     String protocol, String volumeId,
                                     String serverConfigurationFile, boolean defaultServerConfigurationFile,
                                     Archive<?>[] artifacts, String[] mavenArtifacts, Properties properties,
                                     List<InfinispanServerListener> listeners) {
      ServerConfigBuilder builder = new ServerConfigBuilder(serverConfigurationFile, defaultServerConfigurationFile);
      // We ignore the test server directory system property as it would force both to and from version to be ignored
      // when using the version which is pulled from quay.io
      builder.removeProperty(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR);
      builder.runMode(ServerRunMode.CONTAINER);
      builder.numServers(nodeCount);
      builder.expectedServers(expectedCount);
      builder.clusterName(clusterName);
      properties.forEach((k, v) -> {
         if (k instanceof String && v instanceof String) {
            builder.property((String) k, (String) v);
         }
      });
      builder.property("jgroups.version.check", "false");
      if (siteName != null) {
         builder.site(siteName);
      }
      builder.artifacts(artifacts);
      builder.mavenArtifacts(mavenArtifacts);
      builder.property(Server.INFINISPAN_CLUSTER_STACK, protocol);
      listeners.forEach(builder::addListener);
      // If the nodeCount was the same as expected it means it is the start of a fresh cluster. In that case we don't
      // need the join timeout as there shouldn't be any existing nodes in the cluster and don't wait
      builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_REQUIRE_JOIN_TIMEOUT, Boolean.toString(nodeCount != expectedCount));
      if (configuration.useSharedDataMount()) {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_CONTAINER_VOLUME_REQUIRED, "true");
      }

      String versionToUse = versionType == VersionType.TO ? configuration.toVersion() : configuration.fromVersion();
      String name = (siteName != null ? siteName : "") + clusterName + "-" + versionToUse;

      if (versionToUse.startsWith("image://")) {
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME, versionToUse.substring("image://".length()));
      } else if (versionToUse.startsWith("file://")) {
         // Need to strip the file: as testcontainers strips all `file:` occurrences which seems like a bug
         {
            versionToUse = versionToUse.substring("file://".length());
            name = (siteName != null ? siteName : "") + clusterName + "-" + versionToUse;
         }
         builder.property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_DIR, versionToUse);
         // For simplicity trim down to the directory name for the rest of the test
         versionToUse = Path.of(versionToUse).getFileName().toString();

         String imageName;
         if (versionType == VersionType.TO) {
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
      driver.prepare(name);
      // Invoke listener before starting any container.
      // Listener might add extra runtime properties.
      listeners.forEach(l -> l.before(driver));
      if (volumeId != null) {
         if (nodeCount != 1) {
            throw new IllegalArgumentException("nodeCount " + nodeCount + " must be 1 when a volumeId is passed " + volumeId);
         }
         driver.configureImage(name);
         driver.startAdditionalServer(expectedCount, volumeId);
      } else {
         driver.start(name);
      }
      return new ConfigAndDriver(config, driver);
   }

   enum VersionType {
      FROM,
      TO
   }
}
