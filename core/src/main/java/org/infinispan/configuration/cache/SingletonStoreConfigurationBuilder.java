package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

/**
 * SingletonStore is a delegating cache store used for situations when only one 
 * instance in a cluster should interact with the underlying store. The coordinator of the cluster will be responsible for
 * the underlying CacheStore. SingletonStore is a simply facade to a real CacheStore implementation. It always 
 * delegates reads to the real CacheStore.
 * 
 * @author pmuir
 *
 */
public class SingletonStoreConfigurationBuilder extends AbstractLoaderConfigurationChildBuilder<SingletonStoreConfiguration> {

   private boolean enabled = false;
   private long pushStateTimeout = TimeUnit.SECONDS.toMillis(10);
   private boolean pushStateWhenCoordinator = true;
   
   SingletonStoreConfigurationBuilder(AbstractLoaderConfigurationBuilder<? extends AbstractLoaderConfiguration> builder) {
      super(builder);
   }

   /**
    * Enable the singleton store cache store
    */
   public SingletonStoreConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   /**
    * If true, the singleton store cache store is enabled.
    */
   public SingletonStoreConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }
   
   /**
    * Enable the singleton store cache store
    */
   public SingletonStoreConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    */
   public SingletonStoreConfigurationBuilder pushStateTimeout(long l) {
      this.pushStateTimeout = l;
      return this;
   }

   /**
    * If true, when a node becomes the coordinator, it will transfer in-memory state to the
    * underlying cache store. This can be very useful in situations where the coordinator crashes
    * and there's a gap in time until the new coordinator is elected.
    */
   public SingletonStoreConfigurationBuilder pushStateWhenCoordinator(boolean b) {
      this.pushStateWhenCoordinator = b;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   SingletonStoreConfiguration create() {
      return new SingletonStoreConfiguration(enabled, pushStateTimeout, pushStateWhenCoordinator);
   }
   
   @Override
   public SingletonStoreConfigurationBuilder read(SingletonStoreConfiguration template) {
      this.enabled = template.enabled();
      this.pushStateTimeout = template.pushStateTimeout();
      this.pushStateWhenCoordinator = template.pushStateWhenCoordinator();
      
      return this;
   }

   @Override
   public String toString() {
      return "SingletonStoreConfigurationBuilder{" +
            "enabled=" + enabled +
            ", pushStateTimeout=" + pushStateTimeout +
            ", pushStateWhenCoordinator=" + pushStateWhenCoordinator +
            '}';
   }

}
