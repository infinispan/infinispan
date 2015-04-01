package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.util.Util;

/**
 * SimpleInstanceAttributeCopier. This {@link AttributeCopier} "copies" an instance by creating a new instance
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class SimpleInstanceAttributeCopier<T> implements AttributeCopier<T> {
   public static final AttributeCopier<Object> INSTANCE = new SimpleInstanceAttributeCopier<>();

   private SimpleInstanceAttributeCopier() {
      // Singleton constructor
   }

   @Override
   public T copyAttribute(T attribute) {
      if (attribute == null)
         return null;
      else
         return (T) Util.getInstance(attribute.getClass());
   }

}
