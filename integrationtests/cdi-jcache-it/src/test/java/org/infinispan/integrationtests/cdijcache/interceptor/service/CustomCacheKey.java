package org.infinispan.integrationtests.cdijcache.interceptor.service;

import javax.cache.annotation.GeneratedCacheKey;
import java.lang.reflect.Method;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class CustomCacheKey implements GeneratedCacheKey {

   private static final long serialVersionUID = -2393683631229917970L;

   private final Method method;
   private final Object firstParameter;

   public CustomCacheKey(Method method, Object firstParameter) {
      this.method = method;
      this.firstParameter = firstParameter;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CustomCacheKey that = (CustomCacheKey) o;

      if (firstParameter != null ? !firstParameter.equals(that.firstParameter) : that.firstParameter != null)
         return false;
      if (method != null ? !method.equals(that.method) : that.method != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = method != null ? method.hashCode() : 0;
      result = 31 * result + (firstParameter != null ? firstParameter.hashCode() : 0);
      return result;
   }
}
