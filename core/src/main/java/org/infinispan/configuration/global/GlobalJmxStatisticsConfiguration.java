package org.infinispan.configuration.global;

import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.util.TypedProperties;

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

   public boolean isEnabled() {
      return enabled;
   }

   public String getDomain() {
      return jmxDomain;
   }
   
   public TypedProperties getProperties() {
      return properties;
   }

   public boolean isAllowDuplicateDomains() {
      return allowDuplicateDomains;
   }

   public String getCacheManagerName() {
      return cacheManagerName;
   }

   public MBeanServerLookup getMBeanServerLookup() {
      return mBeanServerLookup;
   }
}