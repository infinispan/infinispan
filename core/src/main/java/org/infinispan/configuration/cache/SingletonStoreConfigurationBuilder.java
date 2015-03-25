package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.SingletonStoreConfiguration.ENABLED;
import static org.infinispan.configuration.cache.SingletonStoreConfiguration.PUSH_STATE_TIMEOUT;
import static org.infinispan.configuration.cache.SingletonStoreConfiguration.PUSH_STATE_WHEN_COORDINATOR;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

public class SingletonStoreConfigurationBuilder<S> extends AbstractStoreConfigurationChildBuilder<S> implements Builder<SingletonStoreConfiguration> {
   private final AttributeSet attributes;

   SingletonStoreConfigurationBuilder(AbstractStoreConfigurationBuilder<? extends AbstractStoreConfiguration, ?> builder) {
      super(builder);
      attributes = SingletonStoreConfiguration.attributeDefinitionSet();
   }

   /**
    * Enable the singleton store cache store
    */
   public SingletonStoreConfigurationBuilder<S> enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * If true, the singleton store cache store is enabled.
    */
   public SingletonStoreConfigurationBuilder<S> enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   /**
    * Enable the singleton store cache store
    */
   public SingletonStoreConfigurationBuilder<S> disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    */
   public SingletonStoreConfigurationBuilder<S> pushStateTimeout(long l) {
      attributes.attribute(PUSH_STATE_TIMEOUT).set(l);
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
      attributes.attribute(PUSH_STATE_WHEN_COORDINATOR).set(b);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      if (globalConfig.transport().transport() == null) {
         throw new CacheConfigurationException("Must have a transport set in the global configuration in " +
               "order to configure a singleton store");
      }
   }

   @Override
   public SingletonStoreConfiguration create() {
      return new SingletonStoreConfiguration(attributes.protect());
   }

   @Override
   public SingletonStoreConfigurationBuilder<S> read(SingletonStoreConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "SingletonStoreConfigurationBuilder [attributes=" + attributes + "]";
   }

}
