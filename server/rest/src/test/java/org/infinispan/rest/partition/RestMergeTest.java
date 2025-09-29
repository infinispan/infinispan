package org.infinispan.rest.partition;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Eventually;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.RestMergeTest")
public class RestMergeTest extends BasePartitionHandlingTest {
   List<RestServerHelper> restServers;
   List<RestClient> restClients;

   public RestMergeTest() {
      numMembersInCluster = 3;
      cacheMode = CacheMode.DIST_SYNC;
      cleanup = CleanupPhase.AFTER_TEST;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = new ConfigurationBuilder();
      dcc.clustering().cacheMode(cacheMode);
      dcc.clustering().partitionHandling().whenSplit(partitionHandling);
      createClusteredCaches(numMembersInCluster, dcc, new TransportFlags().withFD(true).withMerge(true));
      waitForClusterToForm();

      restServers = new ArrayList<>(cacheManagers.size());
      restClients = new ArrayList<>(cacheManagers.size());

      for (EmbeddedCacheManager embeddedCacheManager : cacheManagers) {
         RestServerHelper restServerHelper = new RestServerHelper(embeddedCacheManager);
         restServerHelper.start(TestResourceTracker.getCurrentTestShortName());
         restServers.add(restServerHelper);

         RestClientConfigurationBuilder configurationBuilder = new RestClientConfigurationBuilder();
         configurationBuilder.addServer().host(restServerHelper.getHost()).port(restServerHelper.getPort());
         restClients.add(RestClient.forConfiguration(configurationBuilder.build()));
      }
   }

   @AfterClass(alwaysRun = true)
   public void tearDown() throws Exception {
      for (RestClient restClient : restClients) {
         try {
            restClient.close();
         } catch (Exception e) {
            log.warnf(e, "Failed to close client: %s", restClient);
         }
      }
      for (RestServerHelper restServer : restServers) {
         restServer.stop();
      }
   }

   @Test
   public void testMergeMembers() {
      TestingUtil.waitForNoRebalanceAcrossManagers(managers());

      Eventually.eventually(expectCompleteTopology(cacheManagers.size()));
      PartitionDescriptor p0 = new PartitionDescriptor(0, 1);
      PartitionDescriptor p1 = new PartitionDescriptor(2);
      splitCluster(p0.getNodes(), p1.getNodes());
      eventuallyEquals(2, () -> advancedCache(0).getDistributionManager().getCacheTopology().getActualMembers().size());
      eventuallyEquals(1, () -> advancedCache(2).getDistributionManager().getCacheTopology().getActualMembers().size());

      partition(0).merge(partition(1));

      eventuallyEquals(3, () -> advancedCache(0).getDistributionManager().getCacheTopology().getActualMembers().size());
      eventuallyEquals(3, () -> advancedCache(2).getDistributionManager().getCacheTopology().getActualMembers().size());

      Eventually.eventually(expectCompleteTopology(cacheManagers.size()));
   }

   private Eventually.Condition expectCompleteTopology(int expectedMembers) {
      return () -> {
         for (RestClient c : restClients) {
            try (RestResponse info = c.container().info().toCompletableFuture().get(10, TimeUnit.SECONDS)) {
               assertEquals(200, info.status());
               Json json = Json.read(info.body());
               if (json.at("cluster_members").asJsonList().size() != expectedMembers) {
                  return false;
               }
            }
         }
         return true;
      };
   }
}
