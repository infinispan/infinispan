package org.infinispan.spring.starter.embedded;

import java.util.LinkedHashSet;
import java.util.Set;

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
    * Store values loaded on a cache miss by {@code @Cacheable(sync = true)} with {@code putForExternalRead},
    * so that in invalidation mode they do not invalidate the key on the other nodes. Applies to all caches.
    */
   private boolean putForExternalReadOnLoad = false;

   /**
    * Names of the caches to which {@code putForExternalReadOnLoad} applies, when it is not enabled for all caches.
    */
   private Set<String> putForExternalReadOnLoadCaches = new LinkedHashSet<>();

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

   /**
    * @return whether values loaded on a cache miss are stored with {@code putForExternalRead} in all caches
    */
   public boolean isPutForExternalReadOnLoad() {
      return putForExternalReadOnLoad;
   }

   /**
    * @param putForExternalReadOnLoad whether values loaded on a cache miss are stored with
    *                                 {@code putForExternalRead} in all caches
    */
   public void setPutForExternalReadOnLoad(boolean putForExternalReadOnLoad) {
      this.putForExternalReadOnLoad = putForExternalReadOnLoad;
   }

   /**
    * @return the names of the caches in which values loaded on a cache miss are stored with
    * {@code putForExternalRead}
    */
   public Set<String> getPutForExternalReadOnLoadCaches() {
      return putForExternalReadOnLoadCaches;
   }

   /**
    * @param putForExternalReadOnLoadCaches the names of the caches in which values loaded on a cache miss are
    *                                       stored with {@code putForExternalRead}
    */
   public void setPutForExternalReadOnLoadCaches(Set<String> putForExternalReadOnLoadCaches) {
      this.putForExternalReadOnLoadCaches = putForExternalReadOnLoadCaches;
   }

   /**
    * @param cacheName the cache name
    * @return whether {@code putForExternalReadOnLoad} applies to the cache with the given name
    */
   public boolean isPutForExternalReadOnLoad(String cacheName) {
      return putForExternalReadOnLoad || putForExternalReadOnLoadCaches.contains(cacheName);
   }
}
