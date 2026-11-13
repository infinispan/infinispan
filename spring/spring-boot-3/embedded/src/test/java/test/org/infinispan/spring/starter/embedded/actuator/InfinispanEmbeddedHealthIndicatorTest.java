package test.org.infinispan.spring.starter.embedded.actuator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.infinispan.commons.util.Version;
import org.infinispan.health.CacheHealth;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedConfigurationProperties.HealthProperties;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedConfigurationProperties.RebalancingStatus;
import org.infinispan.spring.starter.embedded.actuator.InfinispanEmbeddedHealthIndicator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

public class InfinispanEmbeddedHealthIndicatorTest {

   private static final Address NODE_A = address("node-a", NodeVersion.INSTANCE);
   private static final Address NODE_B = address("node-b", NodeVersion.INSTANCE);
   private static final Address OLD_NODE = address("old-node", NodeVersion.SIXTEEN);

   private EmbeddedCacheManager cacheManager;
   private CacheManagerInfo cacheManagerInfo;
   private ClusterHealth clusterHealth;
   private org.infinispan.health.Health health;
   private HealthProperties properties;

   @BeforeEach
   public void setUp() {
      cacheManager = mock(EmbeddedCacheManager.class);
      cacheManagerInfo = mock(CacheManagerInfo.class);
      clusterHealth = mock(ClusterHealth.class);
      health = mock(org.infinispan.health.Health.class);
      properties = new HealthProperties();

      when(cacheManager.getStatus()).thenReturn(ComponentStatus.RUNNING);
      when(cacheManager.getCacheManagerInfo()).thenReturn(cacheManagerInfo);
      when(cacheManager.getHealth()).thenReturn(health);
      when(cacheManager.getMembers()).thenReturn(List.of(NODE_A, NODE_B));
      when(health.getClusterHealth()).thenReturn(clusterHealth);
      when(health.getCacheHealth()).thenReturn(List.of(cacheHealth("people", HealthStatus.HEALTHY)));
      when(clusterHealth.getHealthStatus()).thenReturn(HealthStatus.HEALTHY);

      when(cacheManagerInfo.getCacheManagerStatus()).thenReturn(ComponentStatus.RUNNING.toString());
      when(cacheManagerInfo.getClusterName()).thenReturn("health-cluster");
      when(cacheManagerInfo.getNodeAddress()).thenReturn("node-a");
      when(cacheManagerInfo.getNodeName()).thenReturn("node-a");
      when(cacheManagerInfo.getVersion()).thenReturn(Version.getVersion());
      when(cacheManagerInfo.getClusterSize()).thenReturn(2);
      when(cacheManagerInfo.isCoordinator()).thenReturn(true);
      when(cacheManagerInfo.getPhysicalAddressesRaw()).thenReturn(List.of("127.0.0.1:7800"));
   }

   @Test
   public void testHealthyClusterIsUp() {
      Health result = health();

      assertThat(result.getStatus()).isEqualTo(Status.UP);
      assertThat(result.getDetails())
            .containsEntry("cluster_health", HealthStatus.HEALTHY.name())
            .containsEntry("cluster_name", "health-cluster")
            .containsEntry("cluster_size", 2)
            .containsEntry("node_address", "node-a")
            .containsEntry("node_name", "node-a")
            .containsEntry("coordinator", true)
            .containsEntry("version", Version.getVersion())
            .containsEntry("cache_manager_status", ComponentStatus.RUNNING.toString())
            .containsEntry("physical_addresses", List.of("127.0.0.1:7800"))
            .containsEntry("caches", Map.of("people", HealthStatus.HEALTHY.name()))
            .containsEntry("rolling_upgrade", false);
   }

   @Test
   public void testMembersCarryTheVersionOfEveryNode() {
      Health result = health();

      assertThat(members(result)).containsExactly(
            Map.of("node_address", "node-a", "version", NodeVersion.INSTANCE.toString()),
            Map.of("node_address", "node-b", "version", NodeVersion.INSTANCE.toString()));
   }

   @Test
   public void testMixedVersionClusterIsAnInPlaceRollingUpgrade() {
      when(cacheManager.getMembers()).thenReturn(List.of(NODE_A, OLD_NODE));

      Health result = health();

      // The cluster keeps serving requests while the upgrade runs, so the status must not flap.
      assertThat(result.getStatus()).isEqualTo(Status.UP);
      assertThat(result.getDetails()).containsEntry("rolling_upgrade", true);
      assertThat(members(result)).containsExactly(
            Map.of("node_address", "node-a", "version", NodeVersion.INSTANCE.toString()),
            Map.of("node_address", "old-node", "version", NodeVersion.SIXTEEN.toString()));
   }

   @Test
   public void testCacheManagerWithoutTransportReportsItselfAsTheOnlyMember() {
      when(cacheManager.getMembers()).thenReturn(null);
      when(cacheManagerInfo.getNodeAddress()).thenReturn("local");
      when(cacheManagerInfo.getClusterSize()).thenReturn(1);

      Health result = health();

      assertThat(result.getStatus()).isEqualTo(Status.UP);
      assertThat(result.getDetails()).containsEntry("cluster_size", 1).containsEntry("rolling_upgrade", false);
      assertThat(members(result)).containsExactly(
            Map.of("node_address", "local", "version", NodeVersion.INSTANCE.toString()));
   }

   @Test
   public void testEmptyMemberListReportsTheLocalNode() {
      when(cacheManager.getMembers()).thenReturn(List.of());
      when(cacheManagerInfo.getNodeAddress()).thenReturn("local");

      assertThat(members(health())).containsExactly(
            Map.of("node_address", "local", "version", NodeVersion.INSTANCE.toString()));
   }

   @Test
   public void testMembershipCanBeDisabled() {
      properties.getMembership().setEnabled(false);

      Health result = health();

      assertThat(result.getStatus()).isEqualTo(Status.UP);
      assertThat(result.getDetails()).doesNotContainKeys("members", "rolling_upgrade");
      assertThat(result.getDetails()).containsEntry("cluster_health", HealthStatus.HEALTHY.name());
   }

   @ParameterizedTest
   @CsvSource({
         "HEALTHY, UP",
         "HEALTHY_REBALANCING, UP",
         "INITIALIZING, OUT_OF_SERVICE",
         "DEGRADED, DOWN",
         "FAILED, DOWN"
   })
   public void testHealthStatusMapping(HealthStatus healthStatus, String expectedStatus) {
      when(clusterHealth.getHealthStatus()).thenReturn(healthStatus);

      Health result = health();

      assertThat(result.getStatus().getCode()).isEqualTo(expectedStatus);
      assertThat(result.getDetails()).containsEntry("cluster_health", healthStatus.name());
   }

   @ParameterizedTest
   @CsvSource({
         "UP, UP",
         "OUT_OF_SERVICE, OUT_OF_SERVICE",
         "DOWN, DOWN"
   })
   public void testRebalancingStatusIsConfigurable(RebalancingStatus configured, String expectedStatus) {
      properties.setRebalancingStatus(configured);
      when(clusterHealth.getHealthStatus()).thenReturn(HealthStatus.HEALTHY_REBALANCING);

      assertThat(health().getStatus().getCode()).isEqualTo(expectedStatus);
   }

   @Test
   public void testUnhealthyCachesAreReported() {
      when(clusterHealth.getHealthStatus()).thenReturn(HealthStatus.DEGRADED);
      when(health.getCacheHealth()).thenReturn(List.of(
            cacheHealth("people", HealthStatus.HEALTHY),
            cacheHealth("orders", HealthStatus.DEGRADED)));

      Health result = health();

      assertThat(result.getStatus()).isEqualTo(Status.DOWN);
      assertThat(result.getDetails()).containsEntry("caches", Map.of(
            "orders", HealthStatus.DEGRADED.name(),
            "people", HealthStatus.HEALTHY.name()));
   }

   @Test
   public void testStoppedCacheManagerIsDown() {
      when(cacheManager.getStatus()).thenReturn(ComponentStatus.TERMINATED);
      when(cacheManagerInfo.getCacheManagerStatus()).thenReturn(ComponentStatus.TERMINATED.toString());

      Health result = health();

      assertThat(result.getStatus()).isEqualTo(Status.DOWN);
      assertThat(result.getDetails())
            .containsEntry("cache_manager_status", ComponentStatus.TERMINATED.toString())
            .doesNotContainKeys("cluster_health", "caches", "members", "rolling_upgrade");
   }

   @Test
   public void testFailureToReadTheHealthIsDown() {
      when(cacheManager.getHealth()).thenThrow(new IllegalStateException("cannot read the health"));

      Health result = health();

      assertThat(result.getStatus()).isEqualTo(Status.DOWN);
      assertThat(result.getDetails()).containsEntry("error",
            "java.lang.IllegalStateException: cannot read the health");
   }

   private Health health() {
      return new InfinispanEmbeddedHealthIndicator(cacheManager, properties).health();
   }

   @SuppressWarnings("unchecked")
   private static List<Map<String, Object>> members(Health health) {
      return (List<Map<String, Object>>) health.getDetails().get("members");
   }

   private static CacheHealth cacheHealth(String name, HealthStatus status) {
      return new TestCacheHealth(name, status);
   }

   private record TestCacheHealth(String name, HealthStatus status) implements CacheHealth {

      @Override
      public String getCacheName() {
         return name;
      }

      @Override
      public HealthStatus getStatus() {
         return status;
      }
   }

   private static Address address(String name, NodeVersion version) {
      return Address.fromExtendedUUID(Address.randomUUID(name, version, null, null, null));
   }
}
