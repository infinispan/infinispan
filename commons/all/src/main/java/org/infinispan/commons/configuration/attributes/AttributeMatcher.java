package org.infinispan.commons.configuration.attributes;

/**
 * A way to match attributes to works with types without the need to subclass them to add the @{@link Matchable} interface.
 *
 * @param <T>
 */
public interface AttributeMatcher<T> {
   AttributeMatcher<Object> TRUE = (o1, o2) -> true;

   static <T> AttributeMatcher<T> alwaysTrue() {
      return (AttributeMatcher<T>) TRUE;
   }

   boolean matches(Attribute<T> o1, Attribute<T> o2);
}
