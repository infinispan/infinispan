package org.infinispan.configuration.cache;

public class SingletonStoreConfiguration {
   
   private final boolean enabled;
   private final long pushStateTimeout;
   private final boolean pushStateWhenCoordinator;
   
   SingletonStoreConfiguration(boolean enabled, long pushStateTimeout, boolean pushStateWhenCoordinator) {
      this.enabled = enabled;
      this.pushStateTimeout = pushStateTimeout;
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }

   public boolean isEnabled() {
      return enabled;
   }

   public long getPushStateTimeout() {
      return pushStateTimeout;
   }

   public boolean isPushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }
   
}
