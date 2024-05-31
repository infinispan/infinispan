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
}
