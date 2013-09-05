package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;

public class SingletonStoreConfigurationBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements Builder<SingletonStoreConfiguration> {

   private boolean enabled = false;
   private long pushStateTimeout = TimeUnit.SECONDS.toMillis(10);
   private boolean pushStateWhenCoordinator = true;

   SingletonStoreConfigurationBuilder(AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder) {
      super(builder);
   }

   /**
    * Enable the singleton store cache store
    */
   public SingletonStoreConfigurationBuilder<S> enable() {
      this.enabled = true;
      return this;
   }

   /**
    * If true, the singleton store cache store is enabled.
    */
   public SingletonStoreConfigurationBuilder<S> enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }

   /**
    * Enable the singleton store cache store
    */
   public SingletonStoreConfigurationBuilder<S> disable() {
      this.enabled = false;
      return this;
   }

   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    */
   public SingletonStoreConfigurationBuilder<S> pushStateTimeout(long l) {
      this.pushStateTimeout = l;
      return this;
   }

   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    */
   public SingletonStoreConfigurationBuilder<S> pushStateTimeout(long l, TimeUnit unit) {
      return pushStateTimeout(unit.toMillis(l));
   }

   /**
    * If true, when a node becomes the coordinator, it will transfer in-memory state to the
    * underlying cache store. This can be very useful in situations where the coordinator crashes
    * and there's a gap in time until the new coordinator is elected.
    */
   public SingletonStoreConfigurationBuilder<S> pushStateWhenCoordinator(boolean b) {
      this.pushStateWhenCoordinator = b;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public SingletonStoreConfiguration create() {
      return new SingletonStoreConfiguration(enabled, pushStateTimeout, pushStateWhenCoordinator);
   }

   @Override
   public SingletonStoreConfigurationBuilder<S> read(SingletonStoreConfiguration template) {
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
