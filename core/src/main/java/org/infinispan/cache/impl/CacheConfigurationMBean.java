package org.infinispan.cache.impl;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.Units;

/**
 * CacheConfigurationMBeanImpl.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@MBean(objectName = "Configuration", description = "Runtime cache configuration attributes")
public class CacheConfigurationMBean {

   private Configuration configuration;

   @Inject
   public void injectDependencies(Configuration configuration) {
      this.configuration = configuration;
   }

   @ManagedAttribute(description = "Gets the eviction size for the cache",
         displayName = "Gets the eviction size for the cache",
         units = Units.NONE,
         displayType = DisplayType.DETAIL, writable = true)
   public long getEvictionSize() {
      return configuration.memory().size();
   }

   public void setEvictionSize(long newSize) {
      configuration.memory().size(newSize);
   }
}
