/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

package org.infinispan.util.reflect;

import org.infinispan.util.ReflectionUtil;
import org.jboss.jandex.AnnotationInstance;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.MethodInfo;
import org.jboss.jandex.Type;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An annotated method, cached to prevent repeated lookup via reflection.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class CachedMethod {
   private final AnnotationInstance annotationInstance;
   private Method method;
   private final Class[] params;
   private final Annotation[][] paramAnnotations;
   private final static Map<ClassInfo, Class<?>> CLASS_CACHE = new ConcurrentHashMap<ClassInfo, Class<?>>();

   CachedMethod(AnnotationInstance annotationInstance, ClassLoader classLoader, MethodInfo methodInfo) throws ClassNotFoundException {
      this.annotationInstance = annotationInstance;
      ClassInfo classInfo = methodInfo.declaringClass();
      Class<?> c;
      if ((c = CLASS_CACHE.get(classInfo)) == null) {
         c = classLoader.loadClass(classInfo.name().toString());
         CLASS_CACHE.put(classInfo, c);
      }
      params = new Class[methodInfo.args().length];
      int cntr = 0;
      for (Type t : methodInfo.args()) params[cntr++] = classLoader.loadClass(t.toString());
      method = ReflectionUtil.findMethod(c, methodInfo.name(), params);
      paramAnnotations = method.getParameterAnnotations();

   }

   public final Object invoke(Object target, Object... params) throws InvocationTargetException, IllegalAccessException {
      return method.invoke(target, params);
   }

   public final Method getReflectMethod() {
      return method;
   }

   public final Class[] getParams() {
      return params;
   }

   public AnnotationInstance getMethodAnnotation() {
      return annotationInstance;
   }

   public final Annotation[][] getParamAnnotations() {
      return paramAnnotations;
   }
}
