package org.infinispan.commons.configuration.attributes;

/**
 * IdentityAttributeCopier. This {@link AttributeCopier} does not copy the source attribute, but
 * returns it, so that the same instance can be shared across multiple configurations. Since this
 * can only be safely done with threadsafe objects which store no state, be very careful when using
 * it.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class IdentityAttributeCopier<T> implements AttributeCopier<T> {
   public static final AttributeCopier<Object> INSTANCE = new IdentityAttributeCopier<>();

   private IdentityAttributeCopier() {
      // Singleton constructor
   }

   @Override
   public T copyAttribute(T attribute) {
      return attribute;
   }

}
