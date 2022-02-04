package org.infinispan.commons.configuration.attributes;

import org.infinispan.commons.logging.Log;

/**
 * AttributeValidator.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@FunctionalInterface
public interface AttributeValidator<T> {
   void validate(T value);

   static <E extends Number> AttributeValidator<E> greaterThanZero(Enum<?> attribute) {
      return value -> {
         if (value.intValue() < 1) {
            throw Log.CONFIG.attributeMustBeGreaterThanZero(value.intValue(), attribute);
         }
      };
   }
}
