package org.infinispan.commons.configuration.attributes;

/**
 * An interface for defining updatable attributes/attributeset. The default implementation is a no-op.
 *
 * @author Tristan Tarrant
 * @since 13.0
 */

public interface Updatable<T> {

   /**
    * Updates the mutable part of this instance with the values of the other instance
    * @param other
    */
   default void update(String parentName, T other) {
      // Do nothing
   }

   /**
    * Verifies that updating the mutable part of this instance with the values of the other instance is possible
    * @param other
    */
   default void validateUpdate(String parentName, T other) {
      // Do nothing
   }
}
