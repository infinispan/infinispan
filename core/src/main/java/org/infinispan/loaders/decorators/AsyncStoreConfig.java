package org.infinispan.loaders.decorators;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.ConfigurationElement;
import org.infinispan.config.Dynamic;

/**
 * Configuration for the async cache loader
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ConfigurationElement(name="async", parent="loader")
public class AsyncStoreConfig extends AbstractNamedCacheConfigurationBean {
   boolean enabled;
   int threadPoolSize = 1;
   @Dynamic
   long mapLockTimeout = 5000;

   public boolean isEnabled() {
      return enabled;
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "async",
            description="If true, modifications are stored in the cache store asynchronously.")
   public void setEnabled(boolean enabled) {
      testImmutability("enabled");
      this.enabled = enabled;
   }

   public int getThreadPoolSize() {
      return threadPoolSize;
   }
   
   @ConfigurationAttribute(name = "threadPoolSize", 
            containingElement = "async",
            description="Size of the thread pool whose threads are responsible for applying the modifications.")
   public void setThreadPoolSize(int threadPoolSize) {
      testImmutability("threadPoolSize");
      this.threadPoolSize = threadPoolSize;
   }

   public long getMapLockTimeout() {
      return mapLockTimeout;
   }

   @ConfigurationAttribute(name = "mapLockTimeout", 
            containingElement = "async",
            description="Lock timeout for access to map containing latest state.")
   public void setMapLockTimeout(long stateLockTimeout) {
      testImmutability("stateLockTimeout");
      this.mapLockTimeout = stateLockTimeout;
   }   
   
   @Override
   public AsyncStoreConfig clone() {
      try {
         return (AsyncStoreConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen!", e);
      }
   }

}
