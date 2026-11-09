package test.org.infinispan.spring.starter.embedded.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedConfigurationProperties.HealthProperties;
import org.infinispan.spring.starter.embedded.health.InfinispanEmbeddedHealthIndicator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

/**
 * The membership details are only meaningful once a transport is in place, so they must also be verified on a real
 * cluster and not only on a single node.
 */
public class InfinispanEmbeddedHealthIndicatorClusteredTest {

   private static final String CLUSTER_NAME = "health-indicator-cluster";
   private static final String CACHE_NAME = "people";

   private EmbeddedCacheManager first;
   private EmbeddedCacheManager second;

   @BeforeEach
   public void createCluster() {
      first = createCacheManager("node-a");
      second = createCacheManager("node-b");
      await().atMost(Duration.ofSeconds(30)).until(() -> isStable(first) && isStable(second));
   }

   private static boolean isStable(EmbeddedCacheManager cacheManager) {
      return cacheManager.getMembers().size() == 2
            && cacheManager.getHealth().getClusterHealth().getHealthStatus() == HealthStatus.HEALTHY;
   }

   @AfterEach
   public void destroyCluster() {
      Util.close(first, second);
   }

   @Test
   public void testEveryMemberIsReportedWithItsVersion() {
      for (EmbeddedCacheManager cacheManager : List.of(first, second)) {
         Health health = health(cacheManager, new HealthProperties());

         assertThat(health.getStatus()).isEqualTo(Status.UP);
         assertThat(health.getDetails())
               .containsEntry("cluster_health", HealthStatus.HEALTHY.name())
               .containsEntry("cluster_name", CLUSTER_NAME)
               .containsEntry("cluster_size", 2)
               .containsEntry("caches", Map.of(CACHE_NAME, HealthStatus.HEALTHY.name()))
               // Every member runs the local build, so no in-place rolling upgrade is in progress.
               .containsEntry("rolling_upgrade", false);

         assertThat(members(health)).hasSize(2);
         assertThat(nodeAddresses(health)).containsExactlyInAnyOrder(
               first.getAddress().toString(), second.getAddress().toString());
         assertThat(members(health))
               .allSatisfy(member -> assertThat(member).containsEntry("version", NodeVersion.INSTANCE.toString()));
      }
   }

   @Test
   public void testExactlyOneMemberIsTheCoordinator() {
      assertThat(List.of(health(first, new HealthProperties()), health(second, new HealthProperties())))
            .map(health -> health.getDetails().get("coordinator"))
            .containsExactlyInAnyOrder(true, false);
   }

   @Test
   public void testMembershipIsLeftOutWhenDisabled() {
      HealthProperties properties = new HealthProperties();
      properties.getMembership().setEnabled(false);

      Health health = health(first, properties);

      assertThat(health.getStatus()).isEqualTo(Status.UP);
      assertThat(health.getDetails()).containsEntry("cluster_size", 2).doesNotContainKeys("members", "rolling_upgrade");
   }

   @Test
   public void testLeavingMemberIsRemovedFromTheMembership() {
      second.stop();
      await().atMost(Duration.ofSeconds(30)).until(() -> first.getMembers().size() == 1);

      Health health = health(first, new HealthProperties());

      assertThat(nodeAddresses(health)).containsExactly(first.getAddress().toString());
      assertThat(health.getDetails()).containsEntry("rolling_upgrade", false);
   }

   private static Health health(EmbeddedCacheManager cacheManager, HealthProperties properties) {
      return new InfinispanEmbeddedHealthIndicator(cacheManager, properties).health();
   }

   @SuppressWarnings("unchecked")
   private static List<Map<String, Object>> members(Health health) {
      return (List<Map<String, Object>>) health.getDetails().get("members");
   }

   private static List<Object> nodeAddresses(Health health) {
      return members(health).stream().map(member -> member.get("node_address")).collect(Collectors.toList());
   }

   private static EmbeddedCacheManager createCacheManager(String nodeName) {
      GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
      global.transport().defaultTransport().clusterName(CLUSTER_NAME).nodeName(nodeName);
      global.jmx().disable();

      ConfigurationBuilder cache = new ConfigurationBuilder();
      cache.clustering().cacheMode(CacheMode.DIST_SYNC);

      DefaultCacheManager cacheManager = new DefaultCacheManager(global.build());
      cacheManager.defineConfiguration(CACHE_NAME, cache.build());
      cacheManager.getCache(CACHE_NAME);
      return cacheManager;
   }
}
