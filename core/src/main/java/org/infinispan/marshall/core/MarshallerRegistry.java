package org.infinispan.marshall.core;

import org.infinispan.commons.marshall.Marshaller;

/**
 * A global registry for named {@link Marshaller} instances.
 * <p>
 * The registry maintains a collection of marshallers identified by unique names,
 * including a "default" marshaller that uses the currently configured default marshaller.
 * </p>
 *
 * @author William Burns
 * @since 16.2
 */
public interface MarshallerRegistry {

   /**
    * The name used for the default marshaller entry.
    */
   String DEFAULT_MARSHALLER_NAME = "default";

   /**
    * Retrieves a marshaller by name.
    *
    * @param name the name of the marshaller to retrieve
    * @return the marshaller associated with the given name, or {@code null} if no marshaller is registered
    * @throws IllegalArgumentException if the name is null or empty
    */
   Marshaller getMarshaller(String name);

   /**
    * Checks if a marshaller with the given name is registered.
    *
    * @param name the name to check
    * @return {@code true} if a marshaller is registered with the given name, {@code false} otherwise
    */
   boolean hasMarshaller(String name);

   /**
    * Returns the default marshaller.
    * This is equivalent to calling {@code getMarshaller(DEFAULT_MARSHALLER_NAME)}.
    *
    * @return the default marshaller
    */
   default Marshaller getDefaultMarshaller() {
      return getMarshaller(DEFAULT_MARSHALLER_NAME);
   }
}
