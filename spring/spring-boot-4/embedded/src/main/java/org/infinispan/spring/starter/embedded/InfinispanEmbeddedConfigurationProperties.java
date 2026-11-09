package org.infinispan.spring.starter.embedded;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("infinispan.embedded")
public class InfinispanEmbeddedConfigurationProperties {
   public static final String DEFAULT_CLUSTER_NAME = "default-autoconfigure";

   /**
    * Enable embedded cache.
    */
   private boolean enabled = true;

   /**
    * Reactive mode
    */
   private boolean reactive = false;

   /**
    * The configuration file to use as a template for all caches created.
    */
   private String configXml = "";

   private String machineId = "";

   /**
    * The name of the cluster.
    */
   private String clusterName = DEFAULT_CLUSTER_NAME;

   /**
    * Health indicator configuration.
    */
   private final HealthProperties health = new HealthProperties();

   public String getConfigXml() {
      return configXml;
   }

   public void setConfigXml(String configXml) {
      this.configXml = configXml;
   }

   public String getMachineId() {
      return machineId;
   }

   public void setMachineId(String machineId) {
      this.machineId = machineId;
   }

   public String getClusterName() {
      return clusterName;
   }

   public void setClusterName(String clusterName) {
      this.clusterName = clusterName;
   }

   public boolean isEnabled() {
      return enabled;
   }

   public void setEnabled(boolean enabled) {
      this.enabled = enabled;
   }

   public void setReactive(boolean reactive) {
      this.reactive = reactive;
   }

   public boolean isReactive() {
      return reactive;
   }

   public HealthProperties getHealth() {
      return health;
   }

   /**
    * Configuration of the Actuator health indicator contributed by the embedded starter.
    */
   public static class HealthProperties {

      /**
       * Enable the health indicator for the embedded Cache Manager.
       */
      private boolean enabled = true;

      /**
       * The status to report while the cluster is rebalancing. Rebalancing clusters keep serving requests, so the
       * default keeps the application up and avoids a readiness probe flapping throughout a rolling upgrade.
       */
      private RebalancingStatus rebalancingStatus = RebalancingStatus.UP;

      /**
       * Cluster membership details.
       */
      private final MembershipProperties membership = new MembershipProperties();

      public boolean isEnabled() {
         return enabled;
      }

      public void setEnabled(boolean enabled) {
         this.enabled = enabled;
      }

      public RebalancingStatus getRebalancingStatus() {
         return rebalancingStatus;
      }

      public void setRebalancingStatus(RebalancingStatus rebalancingStatus) {
         this.rebalancingStatus = rebalancingStatus;
      }

      public MembershipProperties getMembership() {
         return membership;
      }
   }

   /**
    * Configuration of the cluster membership details of the health indicator.
    */
   public static class MembershipProperties {

      /**
       * Include the address and the version of every cluster member in the health details. The membership is
       * resolved from the local cluster view, so no cluster-wide request is performed.
       */
      private boolean enabled = true;

      public boolean isEnabled() {
         return enabled;
      }

      public void setEnabled(boolean enabled) {
         this.enabled = enabled;
      }
   }

   /**
    * The health statuses that can be reported while the cluster is rebalancing.
    */
   public enum RebalancingStatus {
      UP,
      OUT_OF_SERVICE,
      DOWN
   }
}
