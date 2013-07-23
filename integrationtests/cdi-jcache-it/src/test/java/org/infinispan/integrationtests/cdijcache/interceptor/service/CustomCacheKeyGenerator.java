package org.infinispan.integrationtests.cdijcache.interceptor.service;

import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.GeneratedCacheKey;
import java.lang.annotation.Annotation;

/**
 * @author Kevin Pollet <kevin.pollet@serli.com> (C) 2011 SERLI
 */
public class CustomCacheKeyGenerator implements CacheKeyGenerator {

   @Override
   public GeneratedCacheKey generateCacheKey(CacheKeyInvocationContext<? extends Annotation> cacheKeyInvocationContext) {
      return new CustomCacheKey(
            cacheKeyInvocationContext.getMethod(),
            cacheKeyInvocationContext.getAllParameters()[0].getValue()
      );
   }
}
