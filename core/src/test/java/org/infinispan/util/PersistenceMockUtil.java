package org.infinispan.util;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.test.BlockHoundHelper;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.ConfigurationManager;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.SingleSegmentKeyPartitioner;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.TestComponentAccessors;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.TestModuleRepository;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.BlockingManagerImpl;
import org.infinispan.util.concurrent.WithinThreadExecutor;

/**
 * Util class that mocks {@link org.infinispan.AdvancedCache} and {@link org.infinispan.persistence.spi.InitializationContext}
 * for {@link org.infinispan.persistence.spi.CacheWriter} and {@link org.infinispan.persistence.spi.CacheLoader}
 *
 * @author pedro
 * @since 7.0
 */
public class PersistenceMockUtil {

   public static class InvocationContextBuilder {
      private final Class<?> testClass;
      private final Configuration configuration;
      private final PersistenceMarshaller persistenceMarshaller;
      private ClassAllowList classAllowList;
      private TimeService timeService = AbstractInfinispanTest.TIME_SERVICE;
      private KeyPartitioner keyPartitioner = SingleSegmentKeyPartitioner.getInstance();

      public InvocationContextBuilder(Class<?> testClass, Configuration configuration, PersistenceMarshaller persistenceMarshaller) {
         this.testClass = testClass;
         this.configuration = configuration;
         this.persistenceMarshaller = persistenceMarshaller;
      }

      public InvocationContextBuilder setTimeService(TimeService timeService) {
         this.timeService = timeService;
         return this;
      }

      public InvocationContextBuilder setClassAllowList(ClassAllowList classAllowList) {
         this.classAllowList = classAllowList;
         return this;
      }

      public InvocationContextBuilder setKeyPartitioner(KeyPartitioner keyPartitioner) {
         this.keyPartitioner = keyPartitioner;
         return this;
      }

      public InitializationContext build() {
         Cache mockCache = mockCache(testClass.getSimpleName(), configuration, timeService, classAllowList);
         BlockingManager blockingManager = new BlockingManagerImpl();
         TestingUtil.inject(blockingManager,
               new TestComponentAccessors.NamedComponent(KnownComponentNames.BLOCKING_EXECUTOR, BlockHoundHelper.allowBlockingExecutor()),
               new TestComponentAccessors.NamedComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, BlockHoundHelper.ensureNonBlockingExecutor()));
         TestingUtil.startComponent(blockingManager);
         MarshalledEntryFactoryImpl mef = new MarshalledEntryFactoryImpl(persistenceMarshaller);
         GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
         global.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory(testClass));
         return new InitializationContextImpl(configuration.persistence().stores().get(0), mockCache,
               keyPartitioner, persistenceMarshaller, timeService, new ByteBufferFactoryImpl(), mef,
               new WithinThreadExecutor(), global.build(), blockingManager);
      }
   }

   public static InitializationContext createContext(Class<?> testClass, Configuration configuration, PersistenceMarshaller marshaller) {
      return createContext(testClass, configuration, marshaller, AbstractInfinispanTest.TIME_SERVICE);
   }

   public static InitializationContext createContext(Class<?> testClass, Configuration configuration, PersistenceMarshaller marshaller, TimeService timeService) {
      return createContext(testClass, configuration, marshaller, timeService, null);
   }

   public static InitializationContext createContext(Class<?> testClass, Configuration configuration, PersistenceMarshaller marshaller,
                                                     TimeService timeService, ClassAllowList allowList) {
      return new InvocationContextBuilder(testClass, configuration, marshaller)
            .setTimeService(timeService)
            .setClassAllowList(allowList)
            .build();
   }

   private static Cache mockCache(String nodeName, Configuration configuration, TimeService timeService, ClassAllowList allowList) {
      String cacheName = "mock-cache";
      AdvancedCache cache = mock(AdvancedCache.class, RETURNS_DEEP_STUBS);

      GlobalConfiguration gc = new GlobalConfigurationBuilder()
                                  .transport().nodeName(nodeName)
                                  .build();

      Set<String> cachesSet = new HashSet<>();
      EmbeddedCacheManager cm = mock(EmbeddedCacheManager.class);
      when(cm.getCacheManagerConfiguration()).thenReturn(gc);
      when(cm.getClassAllowList()).thenReturn(new ClassAllowList());
      GlobalComponentRegistry gcr = new GlobalComponentRegistry(gc, cm, cachesSet, TestModuleRepository.defaultModuleRepository(),
                                                                mock(ConfigurationManager.class));
      BasicComponentRegistry gbcr = gcr.getComponent(BasicComponentRegistry.class);
      gbcr.replaceComponent(TimeService.class.getName(), timeService, true);
      ComponentRegistry registry = new ComponentRegistry(cacheName, configuration, cache, gcr,
                                                         configuration.getClass().getClassLoader());

      when(cache.getClassLoader()).thenReturn(PersistenceMockUtil.class.getClassLoader());
      when(cache.getCacheManager().getCacheManagerConfiguration()).thenReturn(gc);
      when(cache.getCacheManager().getClassAllowList()).thenReturn(allowList);
      when(cache.getName()).thenReturn(cacheName);
      when(cache.getAdvancedCache()).thenReturn(cache);
      when(cache.getComponentRegistry()).thenReturn(registry);
      when(cache.getStatus()).thenReturn(ComponentStatus.RUNNING);
      when(cache.getCacheConfiguration()).thenReturn(configuration);
      return cache;
   }
}
