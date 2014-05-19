package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.hql.ast.spi.PropertyHelper;

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

   private static final TimeZone GMT_TZ = TimeZone.getTimeZone("GMT");

   private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");   //todo [anistor] is there a standard jpa time format?

   protected ObjectPropertyHelper() {
      dateFormat.setTimeZone(GMT_TZ);
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
      final Class<?> propertyType = getPropertyType(entityType, propertyPath);

      if (Date.class.isAssignableFrom(propertyType)) {
         try {
            return dateFormat.parse(value);
         } catch (ParseException e) {
            throw new IllegalArgumentException(e);
         }
      }
      if (Enum.class.isAssignableFrom(propertyType)) {
         return Enum.valueOf((Class<Enum>) propertyType, value);
      }
      if (propertyType == String.class) {
         return value;
      }
      if (propertyType == Character.class || propertyType == char.class) {
         return value.charAt(0);
      }
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
      if (propertyType == Boolean.class || propertyType == boolean.class) {
         return Boolean.valueOf(value);
      }

      return value;
   }

   public abstract Class<?> getPropertyType(String entityType, List<String> propertyPath);

   public abstract boolean hasProperty(String entityType, List<String> propertyPath);

   public abstract boolean hasEmbeddedProperty(String entityType, List<String> propertyPath);

   public abstract EntityNamesResolver getEntityNamesResolver();

   public abstract TypeMetadata getEntityMetadata(String targetTypeName);
}
