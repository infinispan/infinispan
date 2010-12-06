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
