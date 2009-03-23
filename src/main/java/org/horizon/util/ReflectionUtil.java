/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.horizon.util;

import org.horizon.CacheException;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Basic reflection utilities to enhance what the JDK provides.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class ReflectionUtil {
   private static final Log log = LogFactory.getLog(ReflectionUtil.class);

   /**
    * Returns a set of Methods that contain the given method annotation.  This includes all public, protected, package
    * and private methods, as well as those of superclasses.  Note that this does *not* include overridden methods.
    *
    * @param c              class to inspect
    * @param annotationType the type of annotation to look for
    * @return List of Method objects that require injection.
    */
   public static List<Method> getAllMethods(Class c, Class<? extends Annotation> annotationType) {
      List<Method> annotated = new LinkedList<Method>();
      inspectRecursively(c, annotated, annotationType);
      return annotated;
   }

   /**
    * Inspects a class and its superclasses (all the way to {@link Object} for method instances that contain a given
    * annotation. This even identifies private, package and protected methods, not just public ones.
    *
    * @param c
    * @param s
    * @param annotationType
    */
   private static void inspectRecursively(Class c, List<Method> s, Class<? extends Annotation> annotationType) {
      // Superclass first
      if (!c.equals(Object.class)) inspectRecursively(c.getSuperclass(), s, annotationType);

      for (Method m : c.getDeclaredMethods()) {
         // don't bother if this method has already been overridden by a subclass
         if (!alreadyFound(m, s) && m.isAnnotationPresent(annotationType)) {
            s.add(m);
         }
      }
   }

   /**
    * Tests whether a method has already been found, i.e., overridden.
    *
    * @param m method to inspect
    * @param s collection of methods found
    * @return true a method with the same signature already exists.
    */
   private static boolean alreadyFound(Method m, Collection<Method> s) {
      for (Method found : s) {
         if (m.getName().equals(found.getName()) &&
               Arrays.equals(m.getParameterTypes(), found.getParameterTypes()))
            return true;
      }
      return false;
   }

   public static void setValue(Object instance, String fieldName, Object value) {
      try {
         Field f = findFieldRecursively(instance.getClass(), fieldName);
         if (f == null)
            throw new NoSuchMethodException("Cannot find field " + fieldName + " on " + instance.getClass() + " or superclasses");
         f.setAccessible(true);
         f.set(instance, value);
      }
      catch (Exception e) {
         log.error("Unable to set value!", e);
      }
   }

   private static Field findFieldRecursively(Class c, String fieldName) {
      Field f = null;
      try {
         f = c.getDeclaredField(fieldName);
      }
      catch (NoSuchFieldException e) {
         if (!c.equals(Object.class)) f = findFieldRecursively(c.getSuperclass(), fieldName);
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
   public static void invokeAccessibly(Object instance, Method method, Object[] parameters) {
      try {
         method.setAccessible(true);
         method.invoke(instance, parameters);
      }
      catch (Exception e) {
         throw new CacheException("Unable to invoke method " + method + " on object " + //instance +
               (parameters != null ? " with parameters " + Arrays.asList(parameters) : ""), e);
      }
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
      if (f == null) throw new CacheException("Could not find field named: " + fieldName + " on instance :" + instance);
      try {
         f.setAccessible(true);
         return f.get(instance);
      }
      catch (IllegalAccessException iae) {
         throw new CacheException("Cannot access field " + f, iae);
      }
   }

   /**
    * Inspects the class passed in for the class level annotation specified.  If the annotation is not available, this
    * method recursively inspects superclasses and interfaces until it finds the required annotation.
    * <p/>
    * Returns null if the annotation cannot be found.
    *
    * @param clazz class to inspect
    * @param ann   annotation to search for.  Must be a class-level annotation.
    * @return the annotation instance, or null
    */
   @SuppressWarnings("unchecked")
   public static <T extends Annotation> T getAnnotation(Class clazz, Class<T> ann) {
      // first check class
      T a = (T) clazz.getAnnotation(ann);
      if (a != null) return a;

      // check interfaces
      if (!clazz.isInterface()) {
         Class[] interfaces = clazz.getInterfaces();
         for (Class inter : interfaces) {
            a = getAnnotation(inter, ann);
            if (a != null) return a;
         }
      }

      // check superclasses
      Class superclass = clazz.getSuperclass();
      if (superclass == null) return null; // no where else to look
      return getAnnotation(superclass, ann);
   }

   /**
    * Tests whether an annotation is present on a class.  The order tested is: <ul> <li>The class itself</li> <li>All
    * implemented interfaces</li> <li>Any superclasses</li> </ul>
    *
    * @param clazz      class to test
    * @param annotation annotation to look for
    * @return true if the annotation is found, false otherwise
    */
   public static boolean isAnnotationPresent(Class clazz, Class<? extends Annotation> annotation) {
      return getAnnotation(clazz, annotation) != null;
   }
}
