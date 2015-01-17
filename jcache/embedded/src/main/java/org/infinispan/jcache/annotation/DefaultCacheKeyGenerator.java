package org.infinispan.jcache.annotation;

import static org.infinispan.jcache.annotation.Contracts.assertNotNull;

import java.lang.annotation.Annotation;

import javax.cache.annotation.CacheInvocationParameter;
import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.GeneratedCacheKey;
import javax.enterprise.context.ApplicationScoped;

/**
 * Default {@link javax.cache.annotation.CacheKeyGenerator} implementation.
 * By default all key parameters of the intercepted method compose the
 * {@link javax.cache.annotation.CacheKey}.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 */
@ApplicationScoped
public class DefaultCacheKeyGenerator implements CacheKeyGenerator {

   @Override
   public GeneratedCacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
      assertNotNull(cacheKeyInvocationContext, "cacheKeyInvocationContext parameter must not be null");

      final CacheInvocationParameter[] keyParameters = cacheKeyInvocationContext.getKeyParameters();
      final Object[] keyValues = new Object[keyParameters.length];

      for (int i = 0 ; i < keyParameters.length ; i++) {
         keyValues[i] = keyParameters[i].getValue();
      }

      return new DefaultCacheKey(keyValues);
   }

}
