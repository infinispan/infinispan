package org.infinispan.objectfilter.impl.util;

import java.beans.IntrospectionException;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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

   public interface PropertyAccessor {

      //todo [anistor] use this info to validate the query uses the types correctly
      Class<?> getPropertyType();

      /**
       * Indicates if this is a repeated property (ie. array or collection).
       */
      boolean isMultiple();

      Object getValue(Object instance);

      /**
       * Obtains an Iterator over the values of an array, collection or map attribute.
       *
       * @param instance the target instance for accessing the attribute
       * @return the Iterator or null if the attribute is null
       */
      Iterator<Object> getValueIterator(Object instance);

      /**
       * Get the accessor of a nested property.
       *
       * @param propName the name of the nested property
       * @return the accessor of the nested property
       * @throws IntrospectionException if the nested property was not found
       */
      PropertyAccessor getAccessor(String propName) throws IntrospectionException;
   }

   private abstract static class BasePropertyAccessor implements PropertyAccessor {

      @Override
      public PropertyAccessor getAccessor(String propName) throws IntrospectionException {
         return ReflectionHelper.getAccessor(getPropertyType(), propName);
      }
   }

   private static class FieldPropertyAccessor extends BasePropertyAccessor {

      protected final Field field;

      FieldPropertyAccessor(Field field) {
         this.field = field;
         field.setAccessible(true);
      }

      @Override
      public Class<?> getPropertyType() {
         return field.getType();
      }

      public boolean isMultiple() {
         return false;
      }

      public Object getValue(Object instance) {
         try {
            return field.get(instance);
         } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
         }
      }

      public Iterator<Object> getValueIterator(Object instance) {
         throw new UnsupportedOperationException("This property cannot be iterated");
      }
   }

   private static final class ArrayFieldPropertyAccessor extends FieldPropertyAccessor {

      ArrayFieldPropertyAccessor(Field field) {
         super(field);
      }

      public boolean isMultiple() {
         return true;
      }

      public Iterator<Object> getValueIterator(Object instance) {
         Object value = getValue(instance);
         return value == null ? null : new ArrayIterator<>(value);
      }

      @Override
      public Class<?> getPropertyType() {
         return determineElementType(field.getType(), field.getGenericType());
      }
   }

   private static final class CollectionFieldPropertyAccessor extends FieldPropertyAccessor {

      CollectionFieldPropertyAccessor(Field field) {
         super(field);
      }

      public boolean isMultiple() {
         return true;
      }

      public Iterator<Object> getValueIterator(Object instance) {
         Object value = getValue(instance);
         return value == null ? null : ((Collection) value).iterator();
      }

      @Override
      public Class<?> getPropertyType() {
         return determineElementType(field.getType(), field.getGenericType());
      }
   }

   private static final class MapFieldPropertyAccessor extends FieldPropertyAccessor {

      MapFieldPropertyAccessor(Field field) {
         super(field);
      }

      public boolean isMultiple() {
         return true;
      }

      public Iterator<Object> getValueIterator(Object instance) {
         Object value = getValue(instance);
         return value == null ? null : ((Map<?, Object>) value).values().iterator();
      }

      @Override
      public Class<?> getPropertyType() {
         return determineElementType(field.getType(), field.getGenericType());
      }
   }

   private static class MethodPropertyAccessor extends BasePropertyAccessor {

      protected final Method method;

      MethodPropertyAccessor(Method method) {
         this.method = method;
         method.setAccessible(true);
      }

      @Override
      public Class<?> getPropertyType() {
         return method.getReturnType();
      }

      public boolean isMultiple() {
         return false;
      }

      public Object getValue(Object instance) {
         try {
            return method.invoke(instance);
         } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
         }
      }

      public Iterator<Object> getValueIterator(Object instance) {
         throw new UnsupportedOperationException("This property cannot be iterated");
      }
   }

   private static final class ArrayMethodPropertyAccessor extends MethodPropertyAccessor {

      ArrayMethodPropertyAccessor(Method method) {
         super(method);
      }

      public boolean isMultiple() {
         return true;
      }

      public Iterator<Object> getValueIterator(Object instance) {
         Object value = getValue(instance);
         return value == null ? null : new ArrayIterator<>(value);
      }

      @Override
      public Class<?> getPropertyType() {
         return determineElementType(method.getReturnType(), method.getGenericReturnType());
      }
   }

   private static final class CollectionMethodPropertyAccessor extends MethodPropertyAccessor {

      CollectionMethodPropertyAccessor(Method method) {
         super(method);
      }

      public boolean isMultiple() {
         return true;
      }

      public Iterator<Object> getValueIterator(Object instance) {
         Object value = getValue(instance);
         return value == null ? null : ((Collection<Object>) value).iterator();
      }

      @Override
      public Class<?> getPropertyType() {
         return determineElementType(method.getReturnType(), method.getGenericReturnType());
      }
   }

   private static final class MapMethodPropertyAccessor extends MethodPropertyAccessor {

      MapMethodPropertyAccessor(Method method) {
         super(method);
      }

      public boolean isMultiple() {
         return true;
      }

      public Iterator<Object> getValueIterator(Object instance) {
         Object value = getValue(instance);
         return value == null ? null : ((Map<?, Object>) value).values().iterator();
      }

      @Override
      public Class<?> getPropertyType() {
         return determineElementType(method.getReturnType(), method.getGenericReturnType());
      }
   }

   private ReflectionHelper() {
   }

   public static PropertyAccessor getAccessor(Class<?> clazz, String propertyName) throws IntrospectionException {
      if (propertyName == null || propertyName.length() == 0) {
         throw new IllegalArgumentException("Property name cannot be null or empty");
      }
      if (propertyName.indexOf('.') != -1) {
         throw new IllegalArgumentException("The argument cannot be a nested property name");
      }
      String propertyNameSuffix = Character.toUpperCase(propertyName.charAt(0)) + propertyName.substring(1);

      Class<?> c = clazz;
      while (c != null) {
         PropertyAccessor m = getAccessor(c, propertyName, propertyNameSuffix);
         if (m != null) {
            return m;
         }
         c = c.getSuperclass();
      }

      throw new IntrospectionException("Property not found: " + propertyName);
   }

   private static PropertyAccessor getAccessor(Class<?> clazz, String propertyName, String propertyNameSuffix) {
      // try getter method access
      // we need to find a no-arg public "getXyz" or "isXyz" method which has a suitable return type
      try {
         Method m = clazz.getDeclaredMethod("get" + propertyNameSuffix);
         if (Modifier.isPublic(m.getModifiers()) && !m.getReturnType().equals(Void.class)) {
            return getMethodAccessor(m);
         }
      } catch (NoSuchMethodException e) {
         try {
            Method m = clazz.getDeclaredMethod("is" + propertyNameSuffix);
            if (Modifier.isPublic(m.getModifiers()) && (boolean.class.equals(m.getReturnType()) || Boolean.class.equals(m.getReturnType()))) {
               return getMethodAccessor(m);
            }
         } catch (NoSuchMethodException e1) {
            // ignored, continue
         }
      }

      // try field access
      try {
         Field f = clazz.getDeclaredField(propertyName);
         if (f != null && !f.isSynthetic()) {
            return getFieldAccessor(f);
         }
      } catch (NoSuchFieldException e) {
         // ignored, continue
      }

      return null;
   }

   private static PropertyAccessor getFieldAccessor(Field f) {
      Class<?> fieldClass = f.getType();
      if (fieldClass.isArray()) {
         return new ArrayFieldPropertyAccessor(f);
      } else if (Collection.class.isAssignableFrom(fieldClass)) {
         return new CollectionFieldPropertyAccessor(f);
      } else if (Map.class.isAssignableFrom(fieldClass)) {
         return new MapFieldPropertyAccessor(f);
      }
      return new FieldPropertyAccessor(f);
   }

   private static PropertyAccessor getMethodAccessor(Method m) {
      Class<?> fieldClass = m.getReturnType();
      if (fieldClass.isArray()) {
         return new ArrayMethodPropertyAccessor(m);
      } else if (Collection.class.isAssignableFrom(fieldClass)) {
         return new CollectionMethodPropertyAccessor(m);
      } else if (Map.class.isAssignableFrom(fieldClass)) {
         return new MapMethodPropertyAccessor(m);
      }
      return new MethodPropertyAccessor(m);
   }

   private static Class determineElementType(Class<?> type, Type genericType) {
      if (type.isArray()) {
         if (genericType instanceof Class) {
            return type.getComponentType();
         }
         GenericArrayType genericArrayType = (GenericArrayType) genericType;
         Type genericComponentType = genericArrayType.getGenericComponentType();
         if (genericComponentType instanceof ParameterizedType) {
            return (Class) ((ParameterizedType) genericComponentType).getRawType();
         } else {
            return (Class) ((TypeVariable) genericComponentType).getBounds()[0];
         }
      } else if (Collection.class.isAssignableFrom(type)) {
         return determineCollectionElementType(genericType);
      } else if (Map.class.isAssignableFrom(type)) {
         return determineMapValueTypeParam(genericType);
      }
      return null;
   }

   private static Class determineMapValueTypeParam(Type genericType) {
      if (genericType instanceof ParameterizedType) {
         ParameterizedType type = (ParameterizedType) genericType;
         Type fieldArgType = type.getActualTypeArguments()[1];
         if (fieldArgType instanceof TypeVariable) {
            TypeVariable genericComponentType = (TypeVariable) fieldArgType;
            return (Class) genericComponentType.getBounds()[0];
         } else {
            return (Class) fieldArgType;
         }
      } else if (genericType instanceof Class) {
         Class c = (Class) genericType;
         if (c.getGenericSuperclass() != null && Map.class.isAssignableFrom(c.getSuperclass())) {
            Class x = determineMapValueTypeParam(c.getGenericSuperclass());
            if (x != null) {
               return x;
            }
         }
         for (Type t : c.getGenericInterfaces()) {
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

   private static Class determineCollectionElementType(Type genericType) {
      if (genericType instanceof ParameterizedType) {
         ParameterizedType type = (ParameterizedType) genericType;
         Type fieldArgType = type.getActualTypeArguments()[0];
         if (fieldArgType instanceof Class) {
            return (Class) fieldArgType;
         }
         return (Class) ((ParameterizedType) fieldArgType).getRawType();
      } else if (genericType instanceof Class) {
         Class c = (Class) genericType;
         if (c.getGenericSuperclass() != null && Collection.class.isAssignableFrom(c.getSuperclass())) {
            Class x = determineCollectionElementType(c.getGenericSuperclass());
            if (x != null) {
               return x;
            }
         }
         for (Type t : c.getGenericInterfaces()) {
            if (t instanceof Class && Map.class.isAssignableFrom((Class<?>) t)
                  || t instanceof ParameterizedType && Collection.class.isAssignableFrom((Class) ((ParameterizedType) t).getRawType())) {
               Class x = determineCollectionElementType(t);
               if (x != null) {
                  return x;
               }
            }
         }
      }
      return null;
   }
}
