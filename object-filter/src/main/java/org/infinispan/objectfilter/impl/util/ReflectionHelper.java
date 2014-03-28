package org.infinispan.objectfilter.impl.util;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ReflectionHelper {

   private ReflectionHelper() {
   }

   public static Class<?> getPropertyType(Class<?> clazz, String propertyName) {
      try {
         // todo [anistor] also handle getXXX, isXXX accessor methods if present
         Field field = clazz.getDeclaredField(propertyName);
         return field.getType();
      } catch (NoSuchFieldException e) {
         throw new RuntimeException(e);  // TODO [anistor] handle correctly
      }
   }

   public static Object getPropertyValue(Object instance, Class<?> clazz, String propertyName) {
      try {
         // todo [anistor] also handle getXXX, isXXX accessor methods if present
         Field field = clazz.getDeclaredField(propertyName);
         field.setAccessible(true);
         return field.get(instance);
      } catch (NoSuchFieldException e) {
         throw new RuntimeException(e);  // TODO [anistor] handle correctly
      } catch (IllegalAccessException e) {
         throw new RuntimeException(e);  // TODO [anistor] handle correctly
      }
   }

   public static Iterator<?> getFieldIterator(Object instance, Class<?> clazz, String fieldName) {
      Field field;
      try {
         field = clazz.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
         //todo [anistor] need proper handling
         return null;
      }

      Object fieldValue;
      try {
         field.setAccessible(true);
         fieldValue = field.get(instance);
      } catch (IllegalAccessException e) {
         //todo [anistor] need proper handling
         return null;
      }

      Class<?> fieldClass = field.getType();
      if (fieldClass.isArray()) {
         return new ArrayIterator(fieldValue);
      } else if (Collection.class.isAssignableFrom(fieldClass)) {
         return ((Collection) fieldValue).iterator();
      } else if (Map.class.isAssignableFrom(fieldClass)) {
         return ((Map) fieldValue).values().iterator();
      }

      return null;
   }

   //todo [anistor] use this to validate the query
   public static Class getElementType(Class<?> entityClass, String fieldName) {
      try {
         Field field = entityClass.getDeclaredField(fieldName);
         Class<?> fieldClass = field.getType();
         if (fieldClass.isArray()) {
            Type genericType = field.getGenericType();
            if (genericType instanceof Class) {
               return fieldClass.getComponentType();
            }
            GenericArrayType genericArrayType = (GenericArrayType) genericType;
            TypeVariable genericComponentType = (TypeVariable) genericArrayType.getGenericComponentType();
            return (Class) genericComponentType.getBounds()[0];
         } else if (Collection.class.isAssignableFrom(fieldClass)) {
            return determineCollectionElementTypeParam(field.getGenericType());
         } else if (Map.class.isAssignableFrom(fieldClass)) {
            return determineMapValueTypeParam(field.getGenericType());
         }
      } catch (NoSuchFieldException e) {
         //todo [anistor] need proper handling
      }
      return null;
   }

   private static Class determineMapValueTypeParam(Type genericFieldType) {
      if (genericFieldType instanceof ParameterizedType) {
         ParameterizedType type = (ParameterizedType) genericFieldType;
         Type fieldArgType = type.getActualTypeArguments()[1];
         if (fieldArgType instanceof TypeVariable) {
            TypeVariable genericComponentType = (TypeVariable) fieldArgType;
            return (Class) genericComponentType.getBounds()[0];
         } else {
            return (Class) fieldArgType;
         }
      } else if (genericFieldType instanceof Class) {
         Class genericFieldType1 = (Class) genericFieldType;
         if (genericFieldType1.getGenericSuperclass() != null && Map.class.isAssignableFrom(genericFieldType1.getSuperclass())) {
            Class x = determineMapValueTypeParam(genericFieldType1.getGenericSuperclass());
            if (x != null) {
               return x;
            }
         }
         for (Type t : genericFieldType1.getGenericInterfaces()) {
            if (t instanceof Class && Map.class.isAssignableFrom((Class<?>) t)
                  || t instanceof ParameterizedType && Map.class.isAssignableFrom((Class) ((ParameterizedType) t).getRawType())) {
               Class x = determineMapValueTypeParam(t);
               if (x != null) {
                  return x;
               }
            }
         }
      }
      return null;
   }

   private static Class determineCollectionElementTypeParam(Type genericFieldType) {
      if (genericFieldType instanceof ParameterizedType) {
         ParameterizedType type = (ParameterizedType) genericFieldType;
         Type fieldArgType = type.getActualTypeArguments()[0];
         if (fieldArgType instanceof Class) {
            return (Class) fieldArgType;
         }
         return (Class) ((ParameterizedType) fieldArgType).getRawType();
      } else if (genericFieldType instanceof Class) {
         Class genericFieldType1 = (Class) genericFieldType;
         if (genericFieldType1.getGenericSuperclass() != null && Collection.class.isAssignableFrom(genericFieldType1.getSuperclass())) {
            Class x = determineCollectionElementTypeParam(genericFieldType1.getGenericSuperclass());
            if (x != null) {
               return x;
            }
         }
         for (Type t : genericFieldType1.getGenericInterfaces()) {
            if (t instanceof Class && Map.class.isAssignableFrom((Class<?>) t)
                  || t instanceof ParameterizedType && Collection.class.isAssignableFrom((Class) ((ParameterizedType) t).getRawType())) {
               Class x = determineCollectionElementTypeParam(t);
               if (x != null) {
                  return x;
               }
            }
         }
      }
      return null;
   }
}
