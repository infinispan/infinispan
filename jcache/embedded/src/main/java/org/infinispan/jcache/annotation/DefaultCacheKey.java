package org.infinispan.jcache.annotation;

import static java.util.Arrays.deepEquals;
import static java.util.Arrays.deepHashCode;
import static java.util.Arrays.deepToString;

import javax.cache.annotation.GeneratedCacheKey;

/**
 * Default {@link javax.cache.annotation.GeneratedCacheKey} implementation.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
public class DefaultCacheKey implements GeneratedCacheKey {

   private static final long serialVersionUID = 4410523928649671768L;

   private final Object[] parameters;
   private final int hashCode;

   public DefaultCacheKey(Object[] parameters) {
      this.parameters = parameters;
      this.hashCode = deepHashCode(parameters);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DefaultCacheKey that = (DefaultCacheKey) o;

      return deepEquals(parameters, that.parameters);
   }

   @Override
   public int hashCode() {
      return this.hashCode;
   }

   @Override
   public String toString() {
      return new StringBuilder()
            .append("DefaultCacheKey{")
            .append("parameters=").append(parameters == null ? null : deepToString(parameters))
            .append(", hashCode=").append(hashCode)
            .append('}')
            .toString();
   }

}
