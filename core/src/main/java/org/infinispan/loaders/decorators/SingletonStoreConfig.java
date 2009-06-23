package org.infinispan.loaders.decorators;

import org.infinispan.config.AbstractNamedCacheConfigurationBean;
import org.infinispan.config.ConfigurationAttribute;
import org.infinispan.config.ConfigurationElement;

/**
 * Configuration for a singleton store
 *
 * @author Manik Surtani
 * @since 4.0
 */
@ConfigurationElement(name="singletonStore", parent="loader")
public class SingletonStoreConfig extends AbstractNamedCacheConfigurationBean {

   private static final long serialVersionUID = 824251894176131850L;

   boolean singletonStoreEnabled;
   boolean pushStateWhenCoordinator = true;
   long pushStateTimeout = 10000;

   public boolean isSingletonStoreEnabled() {
      return singletonStoreEnabled;
   }

   @ConfigurationAttribute(name = "singletonStoreEnabled", 
            containingElement = "singletonStore",
            description="Switch to enable singleton store")              
   public void setSingletonStoreEnabled(boolean singletonStoreEnabled) {
      testImmutability("singletonStoreEnabled");
      this.singletonStoreEnabled = singletonStoreEnabled;
   }


   public boolean isPushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }

   @ConfigurationAttribute(name = "pushStateWhenCoordinator", 
            containingElement = "singletonStore",
            description="TODO")
   public void setPushStateWhenCoordinator(boolean pushStateWhenCoordinator) {
      testImmutability("pushStateWhenCoordinator");
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }

   public long getPushStateTimeout() {
      return pushStateTimeout;
   }

   @ConfigurationAttribute(name = "pushStateTimeout", 
            containingElement = "singletonStore",
            description="TODO")
   public void setPushStateTimeout(long pushStateTimeout) {
      testImmutability("pushStateTimeout");
      this.pushStateTimeout = pushStateTimeout;
   }

   @Override
   public SingletonStoreConfig clone() {
      try {
         return (SingletonStoreConfig) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should not happen", e);
      }
   }
}
