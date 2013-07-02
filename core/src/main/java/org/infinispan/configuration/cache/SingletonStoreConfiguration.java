package org.infinispan.configuration.cache;

/**
 * SingletonStore is a delegating cache store used for situations when only one instance in a
 * cluster should interact with the underlying store. The coordinator of the cluster will be
 * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real CacheStore
 * implementation. It always delegates reads to the real CacheStore.
 * 
 * @author pmuir
 * 
 */
public class SingletonStoreConfiguration {

   private final boolean enabled;
   private final long pushStateTimeout;
   private final boolean pushStateWhenCoordinator;

   SingletonStoreConfiguration(boolean enabled, long pushStateTimeout, boolean pushStateWhenCoordinator) {
      this.enabled = enabled;
      this.pushStateTimeout = pushStateTimeout;
      this.pushStateWhenCoordinator = pushStateWhenCoordinator;
   }

   /**
    * If true, the singleton store cache store is enabled.
    */
   public boolean enabled() {
      return enabled;
   }

   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    */
   public long pushStateTimeout() {
      return pushStateTimeout;
   }

   /**
    * If true, when a node becomes the coordinator, it will transfer in-memory state to the
    * underlying cache store. This can be very useful in situations where the coordinator crashes
    * and there's a gap in time until the new coordinator is elected.
    */
   public boolean pushStateWhenCoordinator() {
      return pushStateWhenCoordinator;
   }

   @Override
   public String toString() {
      return "SingletonStoreConfiguration{" +
            "enabled=" + enabled +
            ", pushStateTimeout=" + pushStateTimeout +
            ", pushStateWhenCoordinator=" + pushStateWhenCoordinator +
            '}';
   }

}
