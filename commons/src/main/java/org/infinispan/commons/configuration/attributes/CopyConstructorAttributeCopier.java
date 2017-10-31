package org.infinispan.commons.configuration.attributes;

import java.lang.reflect.Constructor;

import org.infinispan.commons.CacheConfigurationException;

/**
 * CopyConstructorAttributeCopier. This {@link AttributeCopier} expects the attribute value class to have a copy constructor
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
public class CopyConstructorAttributeCopier<T> implements AttributeCopier<T> {
   public static final AttributeCopier<Object> INSTANCE = new CopyConstructorAttributeCopier<>();

   private CopyConstructorAttributeCopier() {
      // Singleton constructor
   }

   @Override
   public T copyAttribute(T attribute) {
      try {
         Class<T> klass = (Class<T>) attribute.getClass();
         Constructor<T> constructor = klass.getConstructor(klass);
         return constructor.newInstance(attribute);
      } catch (Exception e) {
         throw new CacheConfigurationException(e);
      }

   }

}
