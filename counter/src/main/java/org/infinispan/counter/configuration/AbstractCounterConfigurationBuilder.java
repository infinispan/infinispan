package org.infinispan.counter.configuration;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.logging.Log;

/**
 * Base counter configuration builder.
 * <p>
 * It allows to configure the counter's name, initial value and the {@link Storage} mode.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
abstract class AbstractCounterConfigurationBuilder<T extends AbstractCounterConfiguration, S extends AbstractCounterConfigurationBuilder<T, S>>
      implements CounterConfigurationBuilder<T, S> {

   private static final Log log = LogFactory.getLog(AbstractCounterConfigurationBuilder.class, Log.class);
   final AttributeSet attributes;
   private final CounterManagerConfigurationBuilder builder;

   AbstractCounterConfigurationBuilder(CounterManagerConfigurationBuilder builder, AttributeSet attributes) {
      this.builder = builder;
      this.attributes = attributes;
   }

   @Override
   public final S name(String name) {
      attributes.attribute(AbstractCounterConfiguration.NAME).set(name);
      return self();
   }

   @Override
   public final S initialValue(long initialValue) {
      attributes.attribute(AbstractCounterConfiguration.INITIAL_VALUE).set(initialValue);
      return self();
   }

   @Override
   public final S storage(Storage mode) {
      attributes.attribute(AbstractCounterConfiguration.STORAGE).set(mode);
      return self();
   }

   @Override
   public void validate() {
      attributes.attributes().forEach(Attribute::validate);
      if (!builder.isGlobalStateEnabled() &&
            attributes.attribute(AbstractCounterConfiguration.STORAGE).get() == Storage.PERSISTENT) {
         throw log.invalidPersistentStorageMode();
      }
   }

   public String name() {
      return attributes.attribute(AbstractCounterConfiguration.NAME).get();
   }

   @Override
   public StrongCounterConfigurationBuilder addStrongCounter() {
      return builder.addStrongCounter();
   }

   @Override
   public WeakCounterConfigurationBuilder addWeakCounter() {
      return builder.addWeakCounter();
   }
}
