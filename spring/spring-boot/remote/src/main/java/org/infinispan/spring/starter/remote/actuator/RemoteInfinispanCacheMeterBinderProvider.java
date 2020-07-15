package org.infinispan.spring.starter.remote.actuator;

import org.infinispan.client.hotrod.RemoteCache;
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
 * @author Katia Aresti, karesti@redhat.com
 * @since 2.1
 */
@Component
@Qualifier(RemoteInfinispanCacheMeterBinderProvider.NAME)
@ConditionalOnClass(name = "org.springframework.boot.actuate.metrics.cache.CacheMeterBinderProvider")
@ConditionalOnProperty(value = "infinispan.remote.enabled", havingValue = "true", matchIfMissing = true)
public class RemoteInfinispanCacheMeterBinderProvider implements CacheMeterBinderProvider<Cache> {

   public static final String NAME = "remoteInfinispanCacheMeterBinderProvider";

   @Override
   public MeterBinder getMeterBinder(Cache cache, Iterable<Tag> tags) {

      if (cache.getNativeCache() instanceof RemoteCache) {
         return new RemoteInfinispanCacheMeterBinder((RemoteCache) cache.getNativeCache(), tags);
      } else {
         return new RemoteInfinispanCacheMeterBinder(null, tags);
      }
   }
}
