package org.infinispan.server.rollingupgrade;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.client.rest.configuration.RestClientConfigurationProperties;
import org.infinispan.commons.api.CacheContainerAdmin.AdminFlag;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.compatibility.Compatibility;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfiguration;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeHandler;
import org.infinispan.testing.Eventually;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Upgrades a cluster through a graceful cluster shutdown instead of a rolling upgrade.
 * <p>
 * Every node runs the source version until the whole cluster shuts down gracefully. The nodes then return one at a time
 * on the target version, each with the global state the source version saved. The two versions never run at the same
 * time.
 * </p>
 */
public class ClusterShutdownUpgradeIT {
   private static final int NODE_COUNT = 3;
   private static final String CACHE_XML = """
         <replicated-cache name="%s">
           <persistence passivation="false">
             <file-store/>
           </persistence>
         </replicated-cache>
         """;

   @Test
   public void testVolatileCacheRecreatedWhileFirstNodeReturns() {
      String cacheName = "volatile-cache";
      RollingUpgradeHandler uh = startOldCluster();
      try {
         RestClient oldNode = uh.rest(0, restClientBuilder());
         createCache(oldNode, cacheName, AdminFlag.VOLATILE);
         assertStatus(204, oldNode.cache(cacheName).put("before", "shutdown"));

         shutdownCluster(uh, oldNode);

         // The first node returns alone and the volatile definition is created again while the others are missing.
         int firstNode = uh.startNewNodeFromStopped(0, 1);
         createCache(uh.rest(firstNode, restClientBuilder()), cacheName, AdminFlag.VOLATILE);

         for (int i = 1; i < NODE_COUNT; i++) {
            uh.startNewNodeFromStopped(i, i + 1);
         }

         for (int i = 0; i < NODE_COUNT; i++) {
            assertCacheData(uh.rest(i, restClientBuilder()), cacheName, i);
         }
      } finally {
         uh.close();
      }
   }

   @Test
   public void testRollbackAfterFirstNodeStartedOnTargetVersion() throws Exception {
      // No user cache: the test is about the saved topology of org.infinispan.CONFIG only.
      RollingUpgradeHandler uh = startOldCluster();
      try {
         shutdownCluster(uh, uh.rest(0, restClientBuilder()));

         // The upgrade is abandoned after only the first node ran the target version.
         int firstNode = uh.startNewNodeFromStopped(0, 1);
         uh.getToDriver().stop(firstNode);

         // Every node returns on the source version, with the same data volumes.
         ContainerInfinispanServerDriver oldDriver = uh.getFromDriver();
         for (int i = 0; i < NODE_COUNT; i++) {
            oldDriver.restart(i);
         }

         for (int i = 0; i < NODE_COUNT; i++) {
            try (RestClient client = restClient(oldDriver.getServerAddress(i))) {
               assertHealthyCluster(client, i);
            }
         }
      } finally {
         uh.close();
      }
   }

   /**
    * Starts a cluster of {@link #NODE_COUNT} nodes on the source version. The tests drive every later step themselves.
    *
    * @return the handler holding the source-version cluster.
    */
   private static RollingUpgradeHandler startOldCluster() {
      RollingUpgradeConfiguration configuration = new RollingUpgradeConfigurationBuilder(
            ClusterShutdownUpgradeIT.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion())
            .useCustomServerConfiguration("configuration/ClusteredServerTest.xml")
            .jgroupsProtocol("test-tcp")
            .nodeCount(NODE_COUNT)
            .handlers(uh -> { }, uh -> true)
            .build();
      Assumptions.assumeFalse(Compatibility.INSTANCE.isCompatibilitySkip(configuration), "Incompatible test");
      return RollingUpgradeHandler.startOldCluster(configuration);
   }

   /**
    * Creates a replicated cache with a file store.
    *
    * @param client    a REST client connected to any node.
    * @param cacheName the cache name.
    * @param flags     the administration flags, such as {@link AdminFlag#VOLATILE}.
    */
   private static void createCache(RestClient client, String cacheName, AdminFlag... flags) {
      RestEntity xml = RestEntity.create(MediaType.APPLICATION_XML, CACHE_XML.formatted(cacheName));
      assertStatus(200, client.cache(cacheName).createWithConfiguration(xml, flags));
   }

   /**
    * Shuts the whole source-version cluster down gracefully and waits until every node has stopped.
    *
    * @param uh     the handler that started the source-version cluster.
    * @param client a REST client connected to any source-version node.
    */
   private static void shutdownCluster(RollingUpgradeHandler uh, RestClient client) {
      sync(client.cluster().stop(), 5, TimeUnit.MINUTES).close();
      ContainerInfinispanServerDriver driver = uh.getFromDriver();
      Eventually.eventually("Cluster did not shut down within timeout",
            () -> IntStream.range(0, NODE_COUNT).noneMatch(driver::isRunning),
            driver.getTimeout(), 1, TimeUnit.SECONDS);
   }

   /**
    * Asserts that a node defines the cache and serves the entry written before the shutdown.
    *
    * @param client    a REST client connected to the node.
    * @param cacheName the cache name.
    * @param node      the index of the node, for the assertion messages.
    */
   private static void assertCacheData(RestClient client, String cacheName, int node) {
      Eventually.eventually("Cache " + cacheName + " not available on node " + node, () -> {
         try (RestResponse response = sync(client.cache(cacheName).get("before"))) {
            return response.status() == 200;
         }
      });
      try (RestResponse response = sync(client.cache(cacheName).get("before"))) {
         assertThat(response.body()).as("Value of before on node %d", node).isEqualTo("shutdown");
      }
   }

   /**
    * Asserts that a node sees a healthy cluster with every node.
    *
    * @param client a REST client connected to the node.
    * @param node   the index of the node, for the assertion messages.
    */
   private static void assertHealthyCluster(RestClient client, int node) {
      Eventually.eventually("Cluster not healthy with " + NODE_COUNT + " nodes on node " + node, () -> {
         try (RestResponse response = sync(client.container().health())) {
            if (response.status() != 200)
               return false;
            Json health = Json.read(response.body()).at("cluster_health");
            return "HEALTHY".equals(health.at("health_status").asString())
                  && health.at("number_of_nodes").asInteger() == NODE_COUNT;
         }
      });
   }

   /**
    * Creates a REST client for one node, outside the handler's client cache.
    *
    * @param address the node address.
    * @return a REST client connected only to that node, which the caller closes.
    */
   private static RestClient restClient(InetAddress address) {
      RestClientConfigurationBuilder builder = restClientBuilder();
      builder.addServer().host(address.getHostAddress()).port(11222);
      return RestClient.forConfiguration(builder.build());
   }

   private static RestClientConfigurationBuilder restClientBuilder() {
      TestUser user = TestUser.ADMIN;
      RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder()
            // The cluster shutdown and the recovery can take longer than the default timeouts.
            .socketTimeout(RestClientConfigurationProperties.DEFAULT_SO_TIMEOUT * 60)
            .connectionTimeout(RestClientConfigurationProperties.DEFAULT_CONNECT_TIMEOUT * 60);
      builder.security().authentication().enable().username(user.getUser()).password(user.getPassword());
      return builder;
   }
}
