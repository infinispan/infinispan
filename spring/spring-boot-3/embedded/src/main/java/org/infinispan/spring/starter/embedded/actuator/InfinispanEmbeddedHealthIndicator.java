package org.infinispan.spring.starter.embedded.actuator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.infinispan.health.CacheHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.CacheManagerInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.NodeVersion;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedConfigurationProperties.HealthProperties;
import org.infinispan.spring.starter.embedded.InfinispanEmbeddedConfigurationProperties.RebalancingStatus;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;

/**
 * A {@link HealthIndicator} that reports the state of the embedded Cache Manager, the health of every cache it holds
 * and the version each cluster member runs.
 * <p>
 * The membership details mirror the shape of the {@code GET /v2/cluster/members} endpoint of the Infinispan server so
 * that tooling driving an in-place rolling upgrade can consume both deployment modes the same way. Unlike the server
 * endpoint, the membership is resolved from the local cluster view rather than with a cluster-wide request: every
 * address advertises the version of the node it identifies, so polling this indicator never issues a remote
 * invocation and cannot hang while members are unreachable.
 * </p>
 *
 * @since 16.4
 */
public class InfinispanEmbeddedHealthIndicator extends AbstractHealthIndicator {

   private static final String CACHES = "caches";
   private static final String CACHE_MANAGER_STATUS = "cache_manager_status";
   private static final String CLUSTER_HEALTH = "cluster_health";
   private static final String CLUSTER_NAME = "cluster_name";
   private static final String CLUSTER_SIZE = "cluster_size";
   private static final String COORDINATOR = "coordinator";
   private static final String MEMBERS = "members";
   private static final String NODE_ADDRESS = "node_address";
   private static final String NODE_NAME = "node_name";
   private static final String PHYSICAL_ADDRESSES = "physical_addresses";
   private static final String ROLLING_UPGRADE = "rolling_upgrade";
   private static final String VERSION = "version";

   private static final String UNKNOWN_VERSION = "unknown";

   private final EmbeddedCacheManager cacheManager;
   private final HealthProperties properties;

   public InfinispanEmbeddedHealthIndicator(EmbeddedCacheManager cacheManager, HealthProperties properties) {
      super("Infinispan embedded health check failed");
      this.cacheManager = cacheManager;
      this.properties = properties;
   }

   @Override
   protected void doHealthCheck(Health.Builder builder) {
      CacheManagerInfo info = cacheManager.getCacheManagerInfo();
      builder.withDetail(CACHE_MANAGER_STATUS, info.getCacheManagerStatus())
            .withDetail(CLUSTER_NAME, info.getClusterName())
            .withDetail(NODE_ADDRESS, info.getNodeAddress())
            .withDetail(NODE_NAME, info.getNodeName())
            .withDetail(VERSION, info.getVersion());

      if (!cacheManager.getStatus().allowInvocations()) {
         // The Cache Manager is not running, so asking it for the health would only fail with a lifecycle exception.
         builder.down();
         return;
      }

      // A single read of the cluster view backs both the size and the membership details. Reading the view twice
      // would let a member join or leave in between and report a size taken from one view next to the members of
      // another, which the tooling driving a rolling upgrade would see as a contradictory cluster state.
      List<Address> members = cacheManager.getMembers();

      builder.withDetail(CLUSTER_SIZE, clusterSize(members))
            .withDetail(COORDINATOR, info.isCoordinator())
            // The physical addresses of the local node. Resolving them for the other members would need a
            // cluster-wide request, which this indicator deliberately avoids.
            .withDetail(PHYSICAL_ADDRESSES, info.getPhysicalAddressesRaw());

      var health = cacheManager.getHealth();
      HealthStatus healthStatus = health.getClusterHealth().getHealthStatus();
      builder.status(toStatus(healthStatus))
            .withDetail(CLUSTER_HEALTH, healthStatus.name())
            .withDetail(CACHES, cacheHealth(health.getCacheHealth()));

      if (properties.getMembership().isEnabled()) {
         addMembership(builder, info, members);
      }
   }

   /**
    * The number of nodes in the cluster view. A Cache Manager without a transport has no view and counts as a cluster
    * of one, which is what {@link CacheManagerInfo#getClusterSize()} reports for it as well.
    */
   private static int clusterSize(List<Address> members) {
      return members == null || members.isEmpty() ? 1 : members.size();
   }

   /**
    * Maps the {@link HealthStatus} of the cluster onto the {@link Status} reported to Actuator.
    */
   private Status toStatus(HealthStatus healthStatus) {
      return switch (healthStatus) {
         case HEALTHY -> Status.UP;
         case HEALTHY_REBALANCING -> toStatus(properties.getRebalancingStatus());
         case INITIALIZING -> Status.OUT_OF_SERVICE;
         case DEGRADED, FAILED -> Status.DOWN;
      };
   }

   private static Status toStatus(RebalancingStatus rebalancingStatus) {
      return switch (rebalancingStatus) {
         case UP -> Status.UP;
         case OUT_OF_SERVICE -> Status.OUT_OF_SERVICE;
         case DOWN -> Status.DOWN;
      };
   }

   /**
    * The health of every cache the Cache Manager exposes, keyed by cache name. Private internal caches are left out
    * because the Cache Manager already hides them, but they are still accounted for in the overall cluster health.
    * <p>
    * A cache that is configured but has not been started yet is left out as well. Such a cache has no component
    * registry, so the Cache Manager reports it as {@link HealthStatus#FAILED}, while the cluster health skips it
    * altogether. Reporting it here would show a failed cache next to the {@code cluster_health} that calls the
    * cluster healthy.
    * </p>
    */
   private Map<String, String> cacheHealth(List<CacheHealth> cacheHealths) {
      Map<String, String> result = new TreeMap<>();
      for (CacheHealth cacheHealth : cacheHealths) {
         if (!cacheManager.cacheExists(cacheHealth.getCacheName())) {
            continue;
         }
         result.put(cacheHealth.getCacheName(), cacheHealth.getStatus().name());
      }
      return Collections.unmodifiableMap(result);
   }

   /**
    * Adds the address and the version of every cluster member, in view order, along with a flag telling whether the
    * members run different versions, which is the case while an in-place rolling upgrade is in progress. A Cache
    * Manager without a transport has no view, so it reports itself as the only member.
    *
    * @param members the cluster view that {@link #doHealthCheck(Health.Builder)} read, so that the membership and the
    *                reported cluster size always describe the same view.
    */
   private static void addMembership(Health.Builder builder, CacheManagerInfo info, List<Address> members) {
      if (members == null || members.isEmpty()) {
         builder.withDetail(MEMBERS, List.of(member(info.getNodeAddress(), NodeVersion.INSTANCE.toString())))
               .withDetail(ROLLING_UPGRADE, false);
         return;
      }

      List<Map<String, Object>> details = new ArrayList<>(members.size());
      Set<NodeVersion> versions = new HashSet<>();
      for (Address member : members) {
         NodeVersion version = member.getVersion();
         // An address that does not advertise a version is no evidence that the members run different versions, so
         // it is rendered as unknown but must not raise the rolling upgrade flag on its own.
         if (version != null) {
            versions.add(version);
         }
         details.add(member(member.toString(), version == null ? UNKNOWN_VERSION : version.toString()));
      }
      builder.withDetail(MEMBERS, Collections.unmodifiableList(details))
            .withDetail(ROLLING_UPGRADE, versions.size() > 1);
   }

   private static Map<String, Object> member(String nodeAddress, String version) {
      Map<String, Object> member = new LinkedHashMap<>(2);
      member.put(NODE_ADDRESS, nodeAddress);
      member.put(VERSION, version);
      return Collections.unmodifiableMap(member);
   }
}
