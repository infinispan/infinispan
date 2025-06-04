package org.infinispan.cache.impl;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;

/**
 * CacheConfigurationMBeanImpl.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.NAMED_CACHE)
@MBean(objectName = "Configuration", description = "Runtime cache configuration attributes")
public class CacheConfigurationMBean {

   @Inject Configuration configuration;

   @ManagedAttribute(description = "Gets the eviction size for the cache",
         displayName = "Gets the eviction size for the cache",
         writable = true)
   public long getEvictionSize() {
      return configuration.memory().maxCount();
   }

   public void setEvictionSize(long newSize) {
      configuration.memory().maxCount(newSize);
   }
}
