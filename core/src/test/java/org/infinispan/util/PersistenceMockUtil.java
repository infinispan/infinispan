package org.infinispan.util;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.impl.SingleSegmentKeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.util.concurrent.WithinThreadExecutor;

/**
 * Util class that mocks {@link org.infinispan.AdvancedCache} and {@link org.infinispan.persistence.spi.InitializationContext}
 * for {@link org.infinispan.persistence.spi.CacheWriter} and {@link org.infinispan.persistence.spi.CacheLoader}
 *
 * @author pedro
 * @since 7.0
 */
public class PersistenceMockUtil {

   public static InitializationContext createContext(String nodeName, Configuration configuration, StreamingMarshaller marshaller) {
      return createContext(nodeName, configuration, marshaller, AbstractInfinispanTest.TIME_SERVICE);
   }

   public static InitializationContext createContext(String nodeName, Configuration configuration, StreamingMarshaller marshaller, TimeService timeService) {
      Cache mockCache = mockCache(nodeName, configuration, timeService);
      return new InitializationContextImpl(configuration.persistence().stores().get(0), mockCache,
                                           SingleSegmentKeyPartitioner.getInstance(), marshaller,
                                           timeService, new ByteBufferFactoryImpl(), new MarshalledEntryFactoryImpl(marshaller),
                                           new WithinThreadExecutor());
   }

   private static Cache mockCache(String nodeName, Configuration configuration, TimeService timeService) {
      String cacheName = "mock-cache";
      AdvancedCache cache = mock(AdvancedCache.class, RETURNS_DEEP_STUBS);

      GlobalConfiguration gc = new GlobalConfigurationBuilder()
                                  .transport().nodeName(nodeName)
                                  .build();

      Set<String> cachesSet = new HashSet<>();
      EmbeddedCacheManager cm = mock(EmbeddedCacheManager.class);
      when(cm.getCacheManagerConfiguration()).thenReturn(gc);
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(gc, cm, cachesSet);
      BasicComponentRegistry gbcr = gcr.getComponent(BasicComponentRegistry.class);
      gbcr.replaceComponent(TimeService.class.getName(), timeService, true);
      ComponentRegistry registry = new ComponentRegistry(cacheName, configuration, cache, gcr,
                                                         configuration.getClass().getClassLoader());

      when(cache.getClassLoader()).thenReturn(PersistenceMockUtil.class.getClassLoader());
      when(cache.getCacheManager().getCacheManagerConfiguration()) .thenReturn(gc);
      when(cache.getName()).thenReturn(cacheName);
      when(cache.getAdvancedCache()).thenReturn(cache);
      when(cache.getComponentRegistry()).thenReturn(registry);
      when(cache.getStatus()).thenReturn(ComponentStatus.RUNNING);
      when(cache.getCacheConfiguration()).thenReturn(configuration);
      return cache;
   }

}
