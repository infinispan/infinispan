package org.infinispan.integrationtests.cdijcache.interceptor.service;

import java.lang.annotation.Annotation;

import javax.cache.annotation.CacheKeyGenerator;
import javax.cache.annotation.CacheKeyInvocationContext;
import javax.cache.annotation.GeneratedCacheKey;

/**
 * @author Kevin Pollet &lt;kevin.pollet@serli.com&gt; (C) 2011 SERLI
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
