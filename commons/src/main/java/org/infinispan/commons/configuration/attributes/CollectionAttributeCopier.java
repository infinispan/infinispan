package org.infinispan.commons.configuration.attributes;

import java.util.HashMap;
import java.util.HashSet;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * CollectionAttributeCopier. This {@link AttributeCopier} can handle a handful of "known"
 * collection types ( {@link HashSet}, {@link HashMap} )
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class CollectionAttributeCopier<T> implements AttributeCopier<T> {
   private static final Log log = LogFactory.getLog(CollectionAttributeCopier.class);
   public static final AttributeCopier<Object> INSTANCE = new CollectionAttributeCopier<>();

   @SuppressWarnings("unchecked")
   @Override
   public T copyAttribute(T attribute) {
      if (attribute == null) {
         return null;
      }
      Class<?> klass = attribute.getClass();
      if (HashSet.class.equals(klass)) {
         return (T) new HashSet<>((HashSet<?>)attribute);
      } else if (HashMap.class.equals(klass)) {
         return (T) new HashMap<>((HashMap<?, ?>)attribute);
      } else {
         throw log.noAttributeCopierForType(klass);
      }
   }

}
