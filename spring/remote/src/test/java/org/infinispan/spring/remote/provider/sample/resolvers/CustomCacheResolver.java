package org.infinispan.spring.remote.provider.sample.resolvers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.interceptor.CacheOperationInvocationContext;
import org.springframework.cache.interceptor.CacheResolver;
import org.springframework.stereotype.Component;

/**
 * Simple implementation of {@link CacheResolver} interface. It returns a single
 * instance of {@link Cache} with name 'custom'.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Component
public class CustomCacheResolver implements CacheResolver {

   private static final String CACHE_NAME = "custom";

   @Autowired(required = true)
   private CacheManager cacheManager;

   @Override
   public Collection<? extends Cache> resolveCaches(CacheOperationInvocationContext<?> cacheOperationInvocationContext) {
      return new ArrayList<Cache>(Arrays.asList(cacheManager.getCache(CACHE_NAME)));
   }
}
