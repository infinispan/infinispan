package org.infinispan.objectfilter.impl.syntax.parser;

import java.text.ParseException;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.syntax.IndexedFieldProvider;
import org.infinispan.objectfilter.impl.util.DateHelper;
import org.infinispan.objectfilter.impl.util.StringHelper;
import org.jboss.logging.Logger;

/**
 * Provides property metadata when dealing with entities.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class ObjectPropertyHelper<TypeMetadata> {

   private static final Log log = Logger.getMessageLogger(Log.class, ObjectPropertyHelper.class.getName());

   /**
    * A map of all types that we consider to be 'primitives'. They are mapped to the equivalent 'boxed' type.
    */
   protected static final Map<Class<?>, Class<?>> primitives = new HashMap<>();

   static {
      primitives.put(java.util.Date.class, java.util.Date.class);
      primitives.put(java.time.Instant.class, java.time.Instant.class);
      primitives.put(String.class, String.class);
      primitives.put(Character.class, Character.class);
      primitives.put(char.class, Character.class);
      primitives.put(Double.class, Double.class);
      primitives.put(double.class, Double.class);
      primitives.put(Float.class, Float.class);
      primitives.put(float.class, Float.class);
      primitives.put(Long.class, Long.class);
      primitives.put(long.class, Long.class);
      primitives.put(Integer.class, Integer.class);
      primitives.put(int.class, Integer.class);
      primitives.put(Short.class, Short.class);
      primitives.put(short.class, Short.class);
      primitives.put(Byte.class, Byte.class);
      primitives.put(byte.class, Byte.class);
      primitives.put(Boolean.class, Boolean.class);
      primitives.put(boolean.class, Boolean.class);
   }

   protected ObjectPropertyHelper() {
   }

   /**
    * Lookup a type by name and return the metadata that represents it.
    *
    * @param typeName the fully qualified type name
    * @return the metadata representation
    */
   public abstract TypeMetadata getEntityMetadata(String typeName);

   /**
    * Returns the given value converted into the type of the given property as determined via the field bridge of the
    * property.
    *
    * @param value        the value to convert
    * @param entityType   the type hosting the property
    * @param propertyPath the name of the property
    * @return the given value converted into the type of the given property
    */
   public Object convertToPropertyType(TypeMetadata entityType, String[] propertyPath, String value) {
      final Class<?> propertyType = getPrimitivePropertyType(entityType, propertyPath);
      if (propertyType == null) {
         // not a primitive, then it is an embedded entity, need to signal an invalid query
         throw log.getPredicatesOnCompleteEmbeddedEntitiesNotAllowedException(StringHelper.join(propertyPath));
      }

      if (Date.class.isAssignableFrom(propertyType)) {
         try {
            return DateHelper.getJpaDateFormat().parse(value);
         } catch (ParseException e) {
            throw log.getInvalidDateLiteralException(value);
         }
      }

      if (Instant.class.isAssignableFrom(propertyType)) {
         return Instant.parse(value);
      }

      if (Enum.class.isAssignableFrom(propertyType)) {
         try {
            return Enum.valueOf((Class<Enum>) propertyType, value);
         } catch (IllegalArgumentException e) {
            throw log.getInvalidEnumLiteralException(value, propertyType.getName());
         }
      }

      if (propertyType == String.class) {
         return value;
      }

      if (propertyType == Character.class || propertyType == char.class) {
         return value.charAt(0);
      }

      try {
         if (propertyType == Double.class || propertyType == double.class) {
            return Double.valueOf(value);
         }
         if (propertyType == Float.class || propertyType == float.class) {
            return Float.valueOf(value);
         }
         if (propertyType == Long.class || propertyType == long.class) {
            return Long.valueOf(value);
         }
         if (propertyType == Integer.class || propertyType == int.class) {
            return Integer.valueOf(value);
         }
         if (propertyType == Short.class || propertyType == short.class) {
            return Short.valueOf(value);
         }
         if (propertyType == Byte.class || propertyType == byte.class) {
            return Byte.valueOf(value);
         }
      } catch (NumberFormatException ex) {
         throw log.getInvalidNumericLiteralException(value);
      }

      if (propertyType == Boolean.class || propertyType == boolean.class) {
         if ("true".equalsIgnoreCase(value)) {
            return true;
         } else if ("false".equalsIgnoreCase(value)) {
            return false;
         } else {
            throw log.getInvalidBooleanLiteralException(value);
         }
      }

      return value;
   }

   /**
    * Returns the type of the primitive property.
    *
    * @param entityType   the TypeMetadata of the entity
    * @param propertyPath the path of the property
    * @return the {@link Class} or {@code null} if not present or not a primitive property
    */
   public abstract Class<?> getPrimitivePropertyType(TypeMetadata entityType, String[] propertyPath);

   public abstract boolean hasProperty(TypeMetadata entityType, String[] propertyPath);

   public abstract boolean hasEmbeddedProperty(TypeMetadata entityType, String[] propertyPath);

   /**
    * Tests if the attribute path contains repeated (collection/array) attributes.
    */
   public abstract boolean isRepeatedProperty(TypeMetadata entityType, String[] propertyPath);

   public IndexedFieldProvider<TypeMetadata> getIndexedFieldProvider() {
      return typeMetadata -> IndexedFieldProvider.NO_INDEXING;
   }

   public abstract List<?> mapPropertyNamePathToFieldIdPath(TypeMetadata type, String[] propertyPath);

   /**
    * Converts the given property value (usually a String representation coming right from the user's query string) into
    * the type expected by the query backend.
    *
    * @param entityType   the entity type owning the property
    * @param propertyPath the path from the entity to the property (will only contain more than one element in case the
    *                     entity is hosted on an embedded entity).
    * @param value        the value of the property
    * @return the property value, converted into the type expected by the query backend
    */
   public Object convertToBackendType(TypeMetadata entityType, String[] propertyPath, Object value) {
      return value;
   }
}
