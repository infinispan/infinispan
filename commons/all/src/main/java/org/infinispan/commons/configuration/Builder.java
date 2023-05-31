package org.infinispan.commons.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Builder. Validates and constructs a configuration bean
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface Builder<T> {
   default void reset() {
      attributes().reset();
   }

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
    * @param template the configuration from which to "clone" this config if needed.
    * @param combine the way attributes and children of this instance and the template should be combined.
    */
   Builder<?> read(T template, Combine combine);

   default Builder<?> read(T template) {
      return read(template, Combine.DEFAULT);
   }

   AttributeSet attributes();
}
