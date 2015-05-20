package org.infinispan.jcache;

import java.lang.annotation.Annotation;
import java.util.Objects;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.GeneratedCacheKey;

/**
 * Custom implementation of {@link javax.cache.annotation.CacheKeyGenerator}. Compatible with methods providing one key parameter.
 *
 * @author Matej Cimbora
 */
public class JCacheCustomKeyGenerator implements CacheKeyGenerator {

   @Override
   public GeneratedCacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
      if (cacheKeyInvocationContext.getKeyParameters().length != 1) {
         throw new IllegalArgumentException("Composed keys are not supported.");
      }
      return new CustomGeneratedCacheKey(cacheKeyInvocationContext.getKeyParameters()[0].getValue());
   }

   public static class CustomGeneratedCacheKey implements GeneratedCacheKey {

      private Object value;

      public CustomGeneratedCacheKey(Object value) {
         if (value == null) {
            throw new IllegalArgumentException("Value needs to be specified.");
         }
         this.value = value;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         CustomGeneratedCacheKey that = (CustomGeneratedCacheKey) o;
         return Objects.equals(this.value, that.value);
      }

      @Override
      public int hashCode() {
         return value.hashCode();
      }

      @Override
      public String toString() {
         return "CustomGeneratedCacheKey{" +
               "value=" + value +
               '}';
      }
   }
}
