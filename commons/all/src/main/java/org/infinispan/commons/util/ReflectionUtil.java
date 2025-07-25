package org.infinispan.commons.util;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.CacheException;

/**
 * Basic reflection utilities to enhance what the JDK provides.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class ReflectionUtil {

   private static final String[] EMPTY_STRING_ARRAY = {};

   private static final Class<?>[] primitives = {int.class, byte.class, short.class, long.class,
                                                 float.class, double.class, boolean.class, char.class};

   private static final Class<?>[] primitiveArrays = {int[].class, byte[].class, short[].class, long[].class,
                                                      float[].class, double[].class, boolean[].class, char[].class};
   public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class[0];

   /**
    * Returns a set of Methods that contain the given method annotation.  This includes all public, protected, package
    * and private methods, as well as those of superclasses.  Note that this does *not* include overridden methods.
    *
    * @param c              class to inspect
    * @param annotationType the type of annotation to look for
    * @return List of Method objects that require injection.
    */
   public static List<Method> getAllMethods(Class<?> c, Class<? extends Annotation> annotationType) {
      List<Method> annotated = new ArrayList<>();
      inspectRecursively(c, annotated, annotationType);
      return annotated;
   }

   public static Method findMethod(Class<?> type, String methodName) {
      try {
         return type.getDeclaredMethod(methodName);
      } catch (NoSuchMethodException e) {
         if (type == Object.class || type.isInterface()) {
            throw new CacheException(e);
         }
         return findMethod(type.getSuperclass(), methodName);
      }
   }

   public static Method findMethod(Class<?> type, String methodName, Class<?>... parameters) {
      try {
         return type.getDeclaredMethod(methodName, parameters);
      } catch (NoSuchMethodException e) {
         if (type == Object.class || type.isInterface()) {
            throw new CacheException(e);
         }
         return findMethod(type.getSuperclass(), methodName, parameters);
      }
   }

   /**
    * Inspects a class and its superclasses (all the way to {@link Object} for method instances that contain a given
    * annotation. This even identifies private, package and protected methods, not just public ones.
    */
   private static void inspectRecursively(Class<?> c, List<Method> s, Class<? extends Annotation> annotationType) {

      for (Method m : c.getDeclaredMethods()) {
         // don't bother if this method has already been overridden by a subclass
         if (notFound(m, s) && m.isAnnotationPresent(annotationType)) {
            s.add(m);
         }
      }

      if (c != Object.class) {
         if (!c.isInterface()) {
            inspectRecursively(c.getSuperclass(), s, annotationType);
         }
         for (Class<?> ifc : c.getInterfaces()) inspectRecursively(ifc, s, annotationType);
      }
   }

   /**
    * Tests whether a method has already been found, i.e., overridden.
    *
    * @param m method to inspect
    * @param s collection of methods found
    * @return true a method with the same signature already exists.
    */
   private static boolean notFound(Method m, Collection<Method> s) {
      for (Method found : s) {
         if (m.getName().equals(found.getName()) &&
               Arrays.equals(m.getParameterTypes(), found.getParameterTypes()))
            return false;
      }
      return true;
   }

   private static Field findFieldRecursively(Class<?> c, String fieldName) {
      Field f = null;
      try {
         f = c.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
         if (c != Object.class) f = findFieldRecursively(c.getSuperclass(), fieldName);
      }
      return f;
   }

   /**
    * Invokes a method using reflection, in an accessible manner (by using {@link Method#setAccessible(boolean)}
    *
    * @param instance   instance on which to execute the method
    * @param method     method to execute
    * @param parameters parameters
    */
   public static Object invokeAccessibly(Object instance, Method method, Object... parameters) {
      method.setAccessible(true);
      return invokeMethod(instance, method, parameters);
   }

   public static Object invokeMethod(Object instance, Method method, Object[] parameters) {
      try {
         return method.invoke(instance, parameters);
      } catch (InvocationTargetException e) {
         Throwable cause = e.getCause() != null ? e.getCause() : e;
         throw new CacheException("Unable to invoke method " + method + " on object of type " + (instance == null ? "null" : instance
                                                                                                                                .getClass().getSimpleName()) +
                                  (parameters != null ? " with parameters " + Arrays.asList(parameters) : ""), cause);
      } catch (Exception e) {
         throw new CacheException("Unable to invoke method " + method + " on object of type " + (instance == null ? "null" : instance
                                                                                                                                .getClass().getSimpleName()) +
                                  (parameters != null ? " with parameters " + Arrays.asList(parameters) : ""), e);
      }
   }

   public static void setAccessible(final Method m) {
      try {
         m.setAccessible(true);
      } catch (Exception e) {
         throw new CacheException("Unable to change method accessibility " + m, e);
      }
   }

   public static void setAccessibly(Object instance, Field field, Object value) {
      field.setAccessible(true);
      setField(instance, field, value);
   }

   public static void setField(Object instance, Field field, Object value) {
      try {
         field.set(instance, value);
      } catch (Exception e) {
         throw new CacheException("Unable to set field " + field.getName() + " on object of type " +
                                  (instance == null ? "null" : instance.getClass().getName()) + " to " + value, e);
      }
   }

   public static Method findGetterForField(Class<?> c, String fieldName) {
      Method retval = findGetterForFieldUsingReflection(c, fieldName);
      if (retval == null) {
         if (c != Object.class) {
            if (!c.isInterface()) {
               retval = findGetterForField(c.getSuperclass(), fieldName);
               if (retval == null) {
                  for (Class<?> ifc : c.getInterfaces()) {
                     retval = findGetterForField(ifc, fieldName);
                     if (retval != null) break;
                  }
               }
            }
         }
      }
      return retval;
   }

   private static Method findGetterForFieldUsingReflection(Class<?> c, String fieldName) {
      for (Method m : c.getDeclaredMethods()) {
         String name = m.getName();
         String s = null;
         if (name.startsWith("get")) {
            s = name.substring(3);
         } else if (name.startsWith("is")) {
            s = name.substring(2);
         }

         if (s != null && s.equalsIgnoreCase(fieldName)) {
            return m;
         }
      }
      return null;
   }

   public static Method findSetterForField(Class<?> c, String fieldName) {
      if (c == Object.class) {
         return null;
      }
      for (Method m : c.getDeclaredMethods()) {
         String name = m.getName();
         String s = null;
         if (name.startsWith("set")) {
            s = name.substring(3);
         }

         if (s != null && s.equalsIgnoreCase(fieldName)) {
            return m;
         }
      }
      // Try parent class until we run out
      return findSetterForField(c.getSuperclass(), fieldName);
   }

   public static String extractFieldName(String setterOrGetter) {
      String field = null;
      if (setterOrGetter.startsWith("set") || setterOrGetter.startsWith("get"))
         field = setterOrGetter.substring(3);
      else if (setterOrGetter.startsWith("is"))
         field = setterOrGetter.substring(2);

      if (field != null && field.length() > 1) {
         StringBuilder sb = new StringBuilder();
         sb.append(Character.toLowerCase(field.charAt(0)));
         if (field.length() > 2) sb.append(field.substring(1));
         return sb.toString();
      }
      return null;
   }


   /**
    * Retrieves the value of a field of an object instance via reflection
    *
    * @param instance  to inspect
    * @param fieldName name of field to retrieve
    * @return a value
    */
   public static Object getValue(Object instance, String fieldName) {
      Field f = findFieldRecursively(instance.getClass(), fieldName);
      if (f == null) throw new CacheException("Could not find field named '" + fieldName + "' on instance " + instance);
      try {
         f.setAccessible(true);
         return f.get(instance);
      } catch (IllegalAccessException iae) {
         throw new CacheException("Cannot access field " + f, iae);
      }
   }

   /**
    * Inspects the class passed in for the class level annotation specified.  If the annotation is not available, this
    * method recursively inspects superclasses and interfaces until it finds the required annotation.
        * Returns null if the annotation cannot be found.
    *
    * @param clazz class to inspect
    * @param ann   annotation to search for.  Must be a class-level annotation.
    * @return the annotation instance, or null
    */
   public static <T extends Annotation> T getAnnotation(Class<?> clazz, Class<T> ann) {
      while (true) {
         // first check class
         T a = clazz.getAnnotation(ann);
         if (a != null) return a;

         // check interfaces
         if (!clazz.isInterface()) {
            Class<?>[] interfaces = clazz.getInterfaces();
            for (Class<?> inter : interfaces) {
               a = getAnnotation(inter, ann);
               if (a != null) return a;
            }
         }

         // check superclasses
         Class<?> superclass = clazz.getSuperclass();
         if (superclass == null) return null; // no where else to look
         clazz = superclass;
      }
   }

   /**
    * Tests whether an annotation is present on a class.  The order tested is: <ul> <li>The class itself</li> <li>All
    * implemented interfaces</li> <li>Any superclasses</li> </ul>
    *
    * @param clazz      class to test
    * @param annotation annotation to look for
    * @return true if the annotation is found, false otherwise
    */
   public static boolean isAnnotationPresent(Class<?> clazz, Class<? extends Annotation> annotation) {
      return getAnnotation(clazz, annotation) != null;
   }

   public static Class<?>[] toClassArray(String[] typeList, ClassLoader classLoader) throws ClassNotFoundException {
      if (typeList == null) return EMPTY_CLASS_ARRAY;
      Class<?>[] retval = new Class[typeList.length];
      int i = 0;
      for (String s : typeList) retval[i++] = getClassForName(s, classLoader);
      return retval;
   }

   public static Class<?> getClassForName(String name, ClassLoader cl) throws ClassNotFoundException {
      try {
         return Util.loadClassStrict(name, cl);
      } catch (ClassNotFoundException cnfe) {
         // Could be a primitive - let's check
         for (Class<?> primitive : primitives) if (name.equals(primitive.getName())) return primitive;
         for (Class<?> primitive : primitiveArrays) if (name.equals(primitive.getName())) return primitive;
      }
      throw new ClassNotFoundException("Class " + name + " cannot be found");
   }

   public static String[] toStringArray(Class<?>[] classes) {
      if (classes == null)
         return EMPTY_STRING_ARRAY;
      else {
         String[] classNames = new String[classes.length];
         for (int i=0; i<classes.length; i++) classNames[i] = classes[i].getName();
         return classNames;
      }
   }

   public static Field getField(String fieldName, Class<?> objectClass) {
      try {
         return objectClass.getDeclaredField(fieldName);
      } catch (NoSuchFieldException e) {
         if (objectClass != Object.class) {
            return getField(fieldName, objectClass.getSuperclass());
         } else {
            return null;
         }
      }
   }

   public static <T> T unwrap(Object obj, Class<T> clazz) {
      if (clazz != null && clazz.isAssignableFrom(obj.getClass()))
         return clazz.cast(obj);

      throw CONTAINER.unableToUnwrap(obj, clazz);
   }

   public static <T> T unwrapAny(Class<T> clazz, Object... objs) {
      if (clazz != null) {
         for (Object o : objs) {
            if (clazz.isAssignableFrom(o.getClass()))
               return clazz.cast(o);
         }
      }

      throw CONTAINER.unableToUnwrapAny(Arrays.toString(objs), clazz);
   }

   public static int getIntAccessibly(Field f, Object instance) {
      try {
         f.setAccessible(true);
         return f.getInt(instance);
      } catch (IllegalAccessException e) {
         throw new RuntimeException(e);
      }
   }
}
