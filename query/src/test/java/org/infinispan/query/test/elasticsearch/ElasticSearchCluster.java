package org.infinispan.query.test.elasticsearch;

import static org.infinispan.commons.util.Util.recursiveFileRemove;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.http.HttpServer;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.testng.Assert;

/**
 * Simple In-JVM Elasticsearch cluster for testing purposes, with local discovery and support for plugins.
 * @since 9.0
 */
public final class ElasticSearchCluster {

   private static final String CLUSTER_NAME_CFG = "cluster.name";
   private static final String DISCOVERY_CFG = "discovery.type";
   private static final String PATH_HOME_CFG = "path.home";
   private static final String REFRESH_INTERVAL_CFG = "index.refresh_interval";
   private static final String TRANSPORT_PORT_CFG = "transport.tcp.port";
   private static final String ALLOCATION_THRESHOLD_CFG = "cluster.routing.allocation.disk.threshold_enabled";
   private static final String RECOVERY_TIMEOUT_CFG = "indices.recovery.retry_delay_network";

   private final List<Node> nodes;
   private final Long timeout;
   private final boolean deleteDataOnExit;

   private ElasticSearchCluster(List<Node> nodes, Long timeout, boolean deleteDataOnExit) {
      this.nodes = nodes;
      this.timeout = timeout;
      this.deleteDataOnExit = deleteDataOnExit;
   }

   public String getConnectionString() {
      HttpServer firstServer = nodes.iterator().next().injector().getInstance(HttpServer.class);
      TransportAddress address = firstServer.info().getAddress().publishAddress();
      return "http://" + address;
   }

   private static class TestNode extends Node {
      TestNode(Settings settings, Collection<Class<? extends Plugin>> classpathPlugins) {
         super(InternalSettingsPreparer.prepareEnvironment(settings, null), Version.CURRENT, classpathPlugins);
      }
   }

   public static class ElasticSearchClusterBuilder {

      private File homeFolder;
      private boolean deleteDataOnExit = true;
      private int numberNodes;
      private final Collection<Class<? extends Plugin>> plugins = new HashSet<>();
      private Long timeoutMs;
      private long refreshInterval;

      public ElasticSearchClusterBuilder withNumberNodes(int numberNodes) {
         this.numberNodes = numberNodes;
         return this;
      }

      public ElasticSearchClusterBuilder waitingForGreen(long timeoutMs) {
         this.timeoutMs = timeoutMs;
         return this;
      }

      public ElasticSearchClusterBuilder refreshInterval(long millis) {
         this.refreshInterval = millis;
         return this;
      }

      public ElasticSearchClusterBuilder withHomeFolder(File homeFolder) {
         this.homeFolder = homeFolder;
         return this;
      }

      public ElasticSearchClusterBuilder addPlugin(Class<? extends Plugin> pluginClass) {
         plugins.add(pluginClass);
         return this;
      }

      public ElasticSearchClusterBuilder keepDataOnExit() {
         this.deleteDataOnExit = false;
         return this;
      }

      private File createTempFile() {
         File pathHome = null;
         try {
            pathHome = Files.createTempDirectory("elasticsearch").toFile();
         } catch (IOException cause) {
            Assert.fail("Cannot create temporary directory", cause);
         }
         return pathHome;
      }

      private Node buildNode(int ordinal) {
         File homeDir = homeFolder != null ? homeFolder : createTempFile();
         Settings settings = Settings.builder()
               .put(PATH_HOME_CFG, homeDir.getAbsolutePath())
               .put(DISCOVERY_CFG, "local")
               .put(CLUSTER_NAME_CFG, Thread.currentThread().getName())
               .put(TRANSPORT_PORT_CFG, 0)
               .put(REFRESH_INTERVAL_CFG, TimeValue.timeValueMillis(refreshInterval))
               .put(ALLOCATION_THRESHOLD_CFG, "false")
               .put(RECOVERY_TIMEOUT_CFG, "500ms")
               .build();
         return new TestNode(settings, plugins);
      }

      public ElasticSearchCluster build() throws IOException {
         List<Node> newNodes = IntStream.range(0, numberNodes).boxed().map(this::buildNode).collect(Collectors.toList());
         return new ElasticSearchCluster(newNodes, timeoutMs, deleteDataOnExit);
      }

   }

   public void start() {
      nodes.forEach(Node::start);
      if (timeout != null) {
         nodes.forEach(n -> waitForGreen(n.client()));
      }
   }

   public void stop() {
      nodes.forEach(node -> {
         node.close();
         if (deleteDataOnExit) recursiveFileRemove(Paths.get(node.settings().get(PATH_HOME_CFG)).toFile());
      });
   }

   private void waitForGreen(Client client) {
      TimeValue waitTime = TimeValue.timeValueMillis(timeout);
      ClusterHealthResponse healthAction = client.admin().cluster().health(Requests.clusterHealthRequest()
            .timeout(waitTime).waitForGreenStatus()).actionGet();
      if (healthAction.isTimedOut()) {
         Assert.fail("Timeout while waiting for Elasticsearch cluster");
      }
   }

}
