package org.infinispan.jcache.annotation;

import static java.util.Collections.addAll;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * An helper class providing useful methods to work with JDK collections.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public final class CollectionsHelper {
   /**
    * Disable instantiation.
    */
   private CollectionsHelper() {
   }

   /**
    * Creates a {@link java.util.Set} with the given elements.
    *
    * @param elements the elements.
    * @param <T>      the element type.
    * @return a new {@link java.util.Set} instance containing the given elements.
    * @throws NullPointerException if parameter elements is {@code null}.
    */
   public static <T> Set<T> asSet(T... elements) {
      final Set<T> resultSet = new LinkedHashSet<T>();
      addAll(resultSet, elements);

      return resultSet;
   }
}
