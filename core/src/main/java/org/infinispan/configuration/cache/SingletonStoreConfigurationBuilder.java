package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

public class SingletonStoreConfigurationBuilder extends AbstractLoaderConfigurationChildBuilder<SingletonStoreConfiguration> {

   private boolean enabled = false;
   private long pushStateTimeout = TimeUnit.SECONDS.toMillis(10);
   private boolean pushStateWhenCoordinator = true;
   
   SingletonStoreConfigurationBuilder(AbstractLoaderConfigurationBuilder<? extends AbstractLoaderConfiguration> builder) {
      super(builder);
   }

   public SingletonStoreConfigurationBuilder enable() {
      this.enabled = true;
      return this;
   }
   
   public SingletonStoreConfigurationBuilder enabled(boolean enabled) {
      this.enabled = enabled;
      return this;
   }
   
   public SingletonStoreConfigurationBuilder disable() {
      this.enabled = false;
      return this;
   }

   public SingletonStoreConfigurationBuilder pushStateTimeout(long l) {
      this.pushStateTimeout = l;
      return this;
   }

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
   
}
