/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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
package org.infinispan.manager;

import org.infinispan.util.ConcurrentWeakKeyHashMap;
import org.infinispan.util.ReflectionUtil;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * Reflection cache for storing results of reflection calls that are particularly expensive.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class ReflectionCache {

   private final ConcurrentMap<ClassClassKey, List<Method>> methodsWithAnnotationCache = new ConcurrentWeakKeyHashMap<ClassClassKey, List<Method>>();

   public List<Method> getAllMethods(Class type, Class<? extends Annotation> annotationType) {
      ClassClassKey key = new ClassClassKey(type, annotationType);
      List<Method> annotated = methodsWithAnnotationCache.get(key);
      if (annotated != null)
         return annotated;

      annotated = ReflectionUtil.getAllMethods(type, annotationType);
      methodsWithAnnotationCache.putIfAbsent(key, annotated);
      return annotated;
   }

   public void stop() {
      methodsWithAnnotationCache.clear();
   }

   private static class ClassClassKey {
      private final Class type;
      private final Class otherType;

      public ClassClassKey(Class type, Class annotationType) {
         this.type = type;
         this.otherType = annotationType;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         ClassClassKey that = (ClassClassKey) o;

         if (otherType != null ? !otherType.equals(that.otherType) : that.otherType != null)
            return false;
         if (type != null ? !type.equals(that.type) : that.type != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = type != null ? type.hashCode() : 0;
         result = 31 * result + (otherType != null ? otherType.hashCode() : 0);
         return result;
      }
   }

}
