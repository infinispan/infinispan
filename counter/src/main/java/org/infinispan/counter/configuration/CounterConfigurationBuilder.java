package org.infinispan.counter.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Self;
import org.infinispan.counter.api.Storage;

/**
 * Base counter configuration builder.
 * <p>
 * It allows to configure the name, initial value and the {@link Storage} mode. The counter's name is required.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface CounterConfigurationBuilder<T extends AbstractCounterConfiguration, S extends CounterConfigurationBuilder<T, S>>
      extends Builder<T>, Self<S> {

   /**
    * Sets the counter's name.
    * <p>
    * This attribute is required.
    *
    * @param name the counter's name.
    */
   S name(String name);

   /**
    * Sets the counter's initial value.
    * <p>
    * Default value is zero.
    *
    * @param initialValue the counter's initial value.
    */
   S initialValue(long initialValue);

   /**
    * Sets the counter's storage mode.
    * <p>
    * Default value is {@link Storage#VOLATILE}.
    *
    * @param mode the counter's storage mode.
    * @see Storage
    */
   S storage(Storage mode);

   /**
    * @return a new builder to configure a strong counter.
    */
   StrongCounterConfigurationBuilder addStrongCounter();

   /**
    * @return a new builder to configure a weak counter.
    */
   WeakCounterConfigurationBuilder addWeakCounter();
}
