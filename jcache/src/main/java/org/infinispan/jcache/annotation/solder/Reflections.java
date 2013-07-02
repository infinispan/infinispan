package org.infinispan.jcache.annotation.solder;

import javax.enterprise.inject.spi.Annotated;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for working with JDK Reflection and also CDI's {@link
 * Annotated} metadata.
 *
 * @author Stuart Douglas
 * @author Pete Muir
 */
public class Reflections {

   public static final Type[] EMPTY_TYPES = {};

   /**
    * <p> Perform a runtime cast. Similar to {@link Class#cast(Object)}, but
    * useful when you do not have a {@link Class} object for type you wish to
    * cast to. </p> <p/> <p> {@link Class#cast(Object)} should be used if
    * possible </p>
    *
    * @param <T> the type to cast to
    * @param obj the object to perform the cast on
    * @return the casted object
    * @throws ClassCastException if the type T is not a subtype of the object
    * @see Class#cast(Object)
    */
   @SuppressWarnings("unchecked")
   public static <T> T cast(Object obj) {
      return (T) obj;
   }

   /**
    * Get all the declared fields on the class hierarchy. This <b>will</b>
    * return overridden fields.
    *
    * @param clazz The class to search
    * @return the set of all declared fields or an empty set if there are none
    */
   public static Set<Field> getAllDeclaredFields(Class<?> clazz) {
      HashSet<Field> fields = new HashSet<Field>();
      for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
         for (Field a : c.getDeclaredFields()) {
            fields.add(a);
         }
      }
      return fields;
   }

   /**
    * Get all the declared methods on the class hierarchy. This <b>will</b>
    * return overridden methods.
    *
    * @param clazz The class to search
    * @return the set of all declared methods or an empty set if there are
    *         none
    */
   public static Set<Method> getAllDeclaredMethods(Class<?> clazz) {
      HashSet<Method> methods = new HashSet<Method>();
      for (Class<?> c = clazz; c != null && c != Object.class; c = c.getSuperclass()) {
         for (Method a : c.getDeclaredMethods()) {
            methods.add(a);
         }
      }
      return methods;
   }

   /**
    * Check the assignability of one type to another, taking into account the
    * actual type arguements
    *
    * @param rawType1             the raw type of the class to check
    * @param actualTypeArguments1 the actual type arguements to check, or an
    *                             empty array if not a parameterized type
    * @param rawType2             the raw type of the class to check
    * @param actualTypeArguments2 the actual type arguements to check, or an
    *                             empty array if not a parameterized type
    * @return boolean
    */
   public static boolean isAssignableFrom(Class<?> rawType1, Type[] actualTypeArguments1, Class<?> rawType2, Type[] actualTypeArguments2) {
      return Types.boxedClass(rawType1).isAssignableFrom(Types.boxedClass(rawType2)) && isAssignableFrom(actualTypeArguments1, actualTypeArguments2);
   }

   public static boolean isAssignableFrom(Type[] actualTypeArguments1, Type[] actualTypeArguments2) {
      for (int i = 0; i < actualTypeArguments1.length; i++) {
         Type type1 = actualTypeArguments1[i];
         Type type2 = Object.class;
         if (actualTypeArguments2.length > i) {
            type2 = actualTypeArguments2[i];
         }
         if (!isAssignableFrom(type1, type2)) {
            return false;
         }
      }
      return true;
   }

   public static boolean isAssignableFrom(Type type1, Type[] types2) {
      for (Type type2 : types2) {
         if (isAssignableFrom(type1, type2)) {
            return true;
         }
      }
      return false;
   }

   public static boolean isAssignableFrom(Type type1, Type type2) {
      if (type1 instanceof Class<?>) {
         Class<?> clazz = (Class<?>) type1;
         if (isAssignableFrom(clazz, EMPTY_TYPES, type2)) {
            return true;
         }
      }
      if (type1 instanceof ParameterizedType) {
         ParameterizedType parameterizedType1 = (ParameterizedType) type1;
         if (parameterizedType1.getRawType() instanceof Class<?>) {
            if (isAssignableFrom((Class<?>) parameterizedType1.getRawType(), parameterizedType1.getActualTypeArguments(), type2)) {
               return true;
            }
         }
      }
      if (type1 instanceof WildcardType) {
         WildcardType wildcardType = (WildcardType) type1;
         if (isTypeBounded(type2, wildcardType.getLowerBounds(), wildcardType.getUpperBounds())) {
            return true;
         }
      }
      if (type2 instanceof WildcardType) {
         WildcardType wildcardType = (WildcardType) type2;
         if (isTypeBounded(type1, wildcardType.getUpperBounds(), wildcardType.getLowerBounds())) {
            return true;
         }
      }
      if (type1 instanceof TypeVariable<?>) {
         TypeVariable<?> typeVariable = (TypeVariable<?>) type1;
         if (isTypeBounded(type2, EMPTY_TYPES, typeVariable.getBounds())) {
            return true;
         }
      }
      if (type2 instanceof TypeVariable<?>) {
         TypeVariable<?> typeVariable = (TypeVariable<?>) type2;
         if (isTypeBounded(type1, typeVariable.getBounds(), EMPTY_TYPES)) {
            return true;
         }
      }
      return false;
   }

   public static boolean isTypeBounded(Type type, Type[] lowerBounds, Type[] upperBounds) {
      if (lowerBounds.length > 0) {
         if (!isAssignableFrom(type, lowerBounds)) {
            return false;
         }
      }
      if (upperBounds.length > 0) {
         if (!isAssignableFrom(upperBounds, type)) {
            return false;
         }
      }
      return true;
   }

   public static boolean isAssignableFrom(Class<?> rawType1, Type[] actualTypeArguments1, Type type2) {
      if (type2 instanceof ParameterizedType) {
         ParameterizedType parameterizedType = (ParameterizedType) type2;
         if (parameterizedType.getRawType() instanceof Class<?>) {
            if (isAssignableFrom(rawType1, actualTypeArguments1, (Class<?>) parameterizedType.getRawType(), parameterizedType.getActualTypeArguments())) {
               return true;
            }
         }
      } else if (type2 instanceof Class<?>) {
         Class<?> clazz = (Class<?>) type2;
         if (isAssignableFrom(rawType1, actualTypeArguments1, clazz, EMPTY_TYPES)) {
            return true;
         }
      } else if (type2 instanceof TypeVariable<?>) {
         TypeVariable<?> typeVariable = (TypeVariable<?>) type2;
         if (isTypeBounded(rawType1, actualTypeArguments1, typeVariable.getBounds())) {
            return true;
         }
      }
      return false;
   }

   public static boolean isAssignableFrom(Type[] types1, Type type2) {
      for (Type type : types1) {
         if (isAssignableFrom(type, type2)) {
            return true;
         }
      }
      return false;
   }

}
