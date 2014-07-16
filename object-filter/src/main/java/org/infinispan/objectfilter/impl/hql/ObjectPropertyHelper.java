package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.PropertyHelper;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.util.StringHelper;
import org.jboss.logging.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public abstract class ObjectPropertyHelper<TypeMetadata> implements PropertyHelper {

   private static final Log log = Logger.getMessageLogger(Log.class, ObjectPropertyHelper.class.getName());

   private static final String DATE_FORMAT = "yyyyMMddHHmmssSSS";   //todo [anistor] is there a standard jpa time format?

   private static final TimeZone GMT_TZ = TimeZone.getTimeZone("GMT");

   protected final EntityNamesResolver entityNamesResolver;

   protected ObjectPropertyHelper(EntityNamesResolver entityNamesResolver) {
      this.entityNamesResolver = entityNamesResolver;
   }

   protected DateFormat getDateFormat() {
      SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
      dateFormat.setTimeZone(GMT_TZ);
      return dateFormat;
   }

   /**
    * Returns the given value converted into the type of the given property as determined via the field bridge of the
    * property.
    *
    * @param value        the value to convert
    * @param entityType   the type hosting the property
    * @param propertyPath the name of the property
    * @return the given value converted into the type of the given property
    */
   @Override
   public Object convertToPropertyType(String entityType, List<String> propertyPath, String value) {
      final Class<?> propertyType = getPrimitivePropertyType(entityType, propertyPath);
      if (propertyType == null) {
         // not a primitive, then it is an embedded entity, need to signal an invalid query
         throw log.getPredicatesOnCompleteEmbeddedEntitiesNotAllowedException(StringHelper.join(propertyPath, "."));
      }

      if (Date.class.isAssignableFrom(propertyType)) {
         try {
            return getDateFormat().parse(value);
         } catch (ParseException e) {
            throw log.getInvalidDateLiteralException(value);
         }
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
         if (value.equalsIgnoreCase("true")) {
            return true;
         } else if (value.equalsIgnoreCase("false")) {
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
    * @param entityType   the FQN of the entity type
    * @param propertyPath the path of the property
    * @return the class or null if not a primitive property
    */
   public abstract Class<?> getPrimitivePropertyType(String entityType, List<String> propertyPath);

   public abstract boolean hasProperty(String entityType, List<String> propertyPath);

   public abstract boolean hasEmbeddedProperty(String entityType, List<String> propertyPath);

   public abstract TypeMetadata getEntityMetadata(String targetTypeName);
}
