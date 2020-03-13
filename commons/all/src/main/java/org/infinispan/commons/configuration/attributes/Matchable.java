package org.infinispan.commons.configuration.attributes;

/**
 * An interface for defining non-strict equality, e.g. such as attributes being of the same type but not necessarily
 * having the same value. The default behaviour delegates to {@link Object#equals(Object)}.
 *
 * @author Tristan Tarrant
 * @since 9.2
 */

public interface Matchable<T> {

   default boolean matches(T other) {
      return equals(other);
   }
}
