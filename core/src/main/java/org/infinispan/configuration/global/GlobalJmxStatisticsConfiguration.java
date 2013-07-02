package org.infinispan.configuration.global;

import org.infinispan.commons.util.TypedProperties;
import org.infinispan.jmx.MBeanServerLookup;

public class GlobalJmxStatisticsConfiguration {

   private final boolean enabled;
   private final String jmxDomain;
   private final MBeanServerLookup mBeanServerLookup;
   private final boolean allowDuplicateDomains;
   private final String cacheManagerName;
   private final TypedProperties properties;
   
   GlobalJmxStatisticsConfiguration(boolean enabled, String jmxDomain, MBeanServerLookup mBeanServerLookup,
         boolean allowDuplicateDomains, String cacheManagerName, TypedProperties properties) {
      this.enabled = enabled;
      this.jmxDomain = jmxDomain;
      this.mBeanServerLookup = mBeanServerLookup;
      this.allowDuplicateDomains = allowDuplicateDomains;
      this.cacheManagerName = cacheManagerName;
      this.properties = properties;
   }

   public boolean enabled() {
      return enabled;
   }

   public String domain() {
      return jmxDomain;
   }
   
   public TypedProperties properties() {
      return properties;
   }

   public boolean allowDuplicateDomains() {
      return allowDuplicateDomains;
   }

   public String cacheManagerName() {
      return cacheManagerName;
   }

   public MBeanServerLookup mbeanServerLookup() {
      return mBeanServerLookup;
   }

   @Override
   public String toString() {
      return "GlobalJmxStatisticsConfiguration{" +
            "allowDuplicateDomains=" + allowDuplicateDomains +
            ", enabled=" + enabled +
            ", jmxDomain='" + jmxDomain + '\'' +
            ", mBeanServerLookup=" + mBeanServerLookup +
            ", cacheManagerName='" + cacheManagerName + '\'' +
            ", properties=" + properties +
            '}';
   }

}