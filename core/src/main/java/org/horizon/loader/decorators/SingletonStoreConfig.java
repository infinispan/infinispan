package org.horizon.loader.decorators;

import org.horizon.config.AbstractNamedCacheConfigurationBean;

/**
 * Configuration for a singleton store
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class SingletonStoreConfig extends AbstractNamedCacheConfigurationBean {

   private static final long serialVersionUID = 824251894176131850L;

   boolean singletonStoreEnabled;
   boolean pushStateWhenCoordinator = true;
   long pushStateTimeout = 10000;

   public boolean isSingletonStoreEnabled() {
      return singletonStoreEnabled;
   }

   public void setSingletonStoreEnabled(boolean singletonStoreEnabled) {
      testImmutability("singletonStoreEnabled");
      this.singletonStoreEnabled = singletonStoreEnabled;
   }


   public boolean isPushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }

   public void setPushStateWhenCoordinator(boolean pushStateWhenCoordinator) {
      testImmutability("pushStateWhenCoordinator");
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }

   public long getPushStateTimeout() {
      return pushStateTimeout;
   }

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
