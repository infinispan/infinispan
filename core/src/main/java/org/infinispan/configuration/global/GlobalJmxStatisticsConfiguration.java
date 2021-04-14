package org.infinispan.configuration.global;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.util.TypedProperties;

/**
 * @deprecated since 10.1.3 Use {@link GlobalJmxConfiguration} instead. This will be removed in next major version.
 */
@Deprecated
public abstract class GlobalJmxStatisticsConfiguration {

   /**
    * @return true if JMX is enabled.
    */
   public abstract boolean enabled();

   public abstract String domain();

   public abstract TypedProperties properties();

   /**
    * @return the cache manager name
    * @deprecated Since 10.1. please use {@link GlobalConfiguration#cacheManagerName()} instead.
    */
   @Deprecated
   public abstract String cacheManagerName();

   public abstract MBeanServerLookup mbeanServerLookup();
}
