package org.infinispan.commons.configuration.attributes;

import static org.infinispan.commons.logging.Log.CONFIG;

import java.util.HashMap;
import java.util.HashSet;

/**
 * CollectionAttributeCopier. This {@link AttributeCopier} can handle a handful of "known"
 * collection types ( {@link HashSet}, {@link HashMap} )
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class CollectionAttributeCopier<T> implements AttributeCopier<T> {
   private static final AttributeCopier<Object> INSTANCE = new CollectionAttributeCopier<>();

   public static <T> CollectionAttributeCopier<T> collectionCopier() {
      return (CollectionAttributeCopier<T>) INSTANCE;
   }

   @SuppressWarnings("unchecked")
   @Override
   public T copyAttribute(T attribute) {
      if (attribute == null) {
         return null;
      }
      Class<?> klass = attribute.getClass();
      if (HashSet.class == klass) {
         return (T) new HashSet<>((HashSet<?>)attribute);
      } else if (HashMap.class == klass) {
         return (T) new HashMap<>((HashMap<?, ?>)attribute);
      } else {
         throw CONFIG.noAttributeCopierForType(klass);
      }
   }

}
