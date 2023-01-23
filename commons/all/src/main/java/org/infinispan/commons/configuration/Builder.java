package org.infinispan.commons.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Builder. Validates and constructs a configuration bean
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface Builder<T> {
   /**
    * Validate the data in this builder before building the configuration bean
    */
   default void validate() {}

   /**
    * Create the configuration bean
    *
    * @return
    */
   T create();

   /**
    * Reads the configuration from an already created configuration bean into this builder.
    * Returns an appropriate builder to allow fluent configuration
    *
    *
    * @param template the configuration from which to "clone" this config if needed.
    *
    */
   Builder<?> read(T template);

   AttributeSet attributes();
}
