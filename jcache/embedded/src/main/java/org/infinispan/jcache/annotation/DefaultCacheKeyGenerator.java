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
 * By default all key parameters including the intercepted method and class names compose the
 * {@link javax.cache.annotation.CacheKey}.
 *
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 * @author Galder Zamarreño
 * @author <a href="mailto:daniel@pfeifer.io">Daniel Pfeifer</a>
 */
@ApplicationScoped
public class DefaultCacheKeyGenerator implements CacheKeyGenerator {

   @Override
   public GeneratedCacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
      assertNotNull(cacheKeyInvocationContext, "cacheKeyInvocationContext parameter must not be null");

      final CacheInvocationParameter[] keyParameters = cacheKeyInvocationContext.getKeyParameters();
      final Object[] keyValues = new Object[keyParameters.length + 2]; // make space for class- and method-name 

      final String methodName = cacheKeyInvocationContext.getMethod().getName();
      final String className = cacheKeyInvocationContext.getMethod().getDeclaringClass().getSimpleName(); 
      keyValues[keyValues.length - 1] = methodName;
      keyValues[keyValues.length - 2] = className;

      for (int i = 0 ; i < keyParameters.length ; i++) {
         keyValues[i] = keyParameters[i].getValue();
      }

      return new DefaultCacheKey(keyValues);
   }

}
