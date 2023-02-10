package org.infinispan.spring.starter.embedded.actuator;

import io.micrometer.core.instrument.binder.cache.JCacheMetrics;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.actuate.metrics.cache.CacheMeterBinderProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.Cache;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;

/**
 * When actuate dependency is found in the classpath, this component links Infinispan cache metrics with Actuator
 *
 * @author Katia Aresti, karesti@redtat.com
 * @since 2.1
 */
@Component
@Qualifier(InfinispanCacheMeterBinderProvider.NAME)
@ConditionalOnClass(name = "org.springframework.boot.actuate.metrics.cache.CacheMeterBinderProvider")
@ConditionalOnProperty(value = "infinispan.embedded.enabled", havingValue = "true", matchIfMissing = true)
public class InfinispanCacheMeterBinderProvider implements CacheMeterBinderProvider<Cache> {

   public static final String NAME = "infinispanCacheMeterBinderProvider";

   @Override
   public MeterBinder getMeterBinder(Cache cache, Iterable<Tag> tags) {
      Object nativeCache = cache.getNativeCache();
      MeterBinder meterBinder = null;
      if (nativeCache instanceof org.infinispan.Cache) {
         meterBinder = new InfinispanCacheMeterBinder((org.infinispan.Cache) nativeCache, tags);
      } else {
         if (nativeCache instanceof javax.cache.Cache){ // for caches like org.infinispan.jcache.embedded.JCache
            meterBinder = new JCacheMetrics((javax.cache.Cache) nativeCache, tags);
         }
      }
      return meterBinder;
   }
}
