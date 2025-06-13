package org.infinispan.util;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.Cache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.io.ByteBufferFactoryImpl;
import org.infinispan.commons.test.BlockHoundHelper;
import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.time.TimeService;
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
import org.infinispan.manager.impl.InternalCacheManager;
import org.infinispan.manager.impl.InternalCacheManagerMock;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.marshall.persistence.impl.MarshalledEntryFactoryImpl;
import org.infinispan.persistence.InitializationContextImpl;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.BlockingManagerImpl;
import org.infinispan.util.concurrent.NonBlockingManager;
import org.infinispan.util.concurrent.NonBlockingManagerImpl;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.mockito.Mockito;

public class PersistenceMockUtil {

   public static class InvocationContextBuilder {
      private final Class<?> testClass;
      private final Configuration configuration;
      private final PersistenceMarshaller persistenceMarshaller;
      private ClassAllowList classAllowList;
      private TimeService timeService = AbstractInfinispanTest.TIME_SERVICE;
      private KeyPartitioner keyPartitioner = SingleSegmentKeyPartitioner.getInstance();
      private NonBlockingManager nonBlockingManager;
      private BlockingManager blockingManager;
      private ScheduledExecutorService timeoutScheduledExecutor;

      public InvocationContextBuilder(Class<?> testClass, Configuration configuration, PersistenceMarshaller persistenceMarshaller) {
         this.testClass = testClass;
         this.configuration = configuration;
         this.persistenceMarshaller = persistenceMarshaller;

         blockingManager = new BlockingManagerImpl();
         TestingUtil.inject(blockingManager,
               new TestComponentAccessors.NamedComponent(KnownComponentNames.BLOCKING_EXECUTOR, BlockHoundHelper.allowBlockingExecutor()),
               new TestComponentAccessors.NamedComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, BlockHoundHelper.ensureNonBlockingExecutor()));
         TestingUtil.startComponent(blockingManager);
         nonBlockingManager = new NonBlockingManagerImpl();
         TestingUtil.inject(nonBlockingManager,
               new TestComponentAccessors.NamedComponent(KnownComponentNames.NON_BLOCKING_EXECUTOR, BlockHoundHelper.ensureNonBlockingExecutor()));
         TestingUtil.startComponent(nonBlockingManager);
         timeoutScheduledExecutor = Mockito.mock(ScheduledExecutorService.class);
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

      public InvocationContextBuilder setBlockingManager(BlockingManager blockingManager) {
         this.blockingManager = blockingManager;
         return this;
      }

      public InvocationContextBuilder setNonBlockingManager(NonBlockingManager nonBlockingManager) {
         this.nonBlockingManager = nonBlockingManager;
         return this;
      }

      public InvocationContextBuilder setScheduledExecutor(ScheduledExecutorService timeoutScheduledExecutor) {
         this.timeoutScheduledExecutor = timeoutScheduledExecutor;
         return this;
      }

      public InitializationContext build() {
         Cache mockCache = mockCache(testClass.getSimpleName(), configuration, timeService, classAllowList, timeoutScheduledExecutor);
         MarshalledEntryFactoryImpl mef = new MarshalledEntryFactoryImpl(persistenceMarshaller);
         GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
         global.globalState().persistentLocation(CommonsTestingUtil.tmpDirectory(testClass));
         return new InitializationContextImpl(configuration.persistence().stores().get(0), mockCache,
               keyPartitioner, persistenceMarshaller, timeService, new ByteBufferFactoryImpl(), mef,
               new WithinThreadExecutor(), global.build(), blockingManager, nonBlockingManager);
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

   private static Cache mockCache(String nodeName, Configuration configuration, TimeService timeService,
                                  ClassAllowList allowList, ScheduledExecutorService timeoutScheduledExecutor) {
      String cacheName = "mock-cache";
      CacheImpl cache = mock(CacheImpl.class, RETURNS_DEEP_STUBS);

      GlobalConfiguration gc = new GlobalConfigurationBuilder()
                                  .transport().nodeName(nodeName)
                                  .build();

      InternalCacheManager cm = InternalCacheManagerMock.mock(gc);
      GlobalComponentRegistry gcr = GlobalComponentRegistry.of(cm);
      when(cm.getCacheManagerConfiguration()).thenReturn(gc);
      when(cm.getClassAllowList()).thenReturn(new ClassAllowList());

      BasicComponentRegistry gbcr = gcr.getComponent(BasicComponentRegistry.class);
      gbcr.replaceComponent(TimeService.class.getName(), timeService, true);
      gbcr.replaceComponent(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, timeoutScheduledExecutor, false);
      ComponentRegistry registry = new ComponentRegistry(cacheName, configuration, cache, gcr,
                                                         configuration.getClass().getClassLoader());
      when(cache.getClassLoader()).thenReturn(PersistenceMockUtil.class.getClassLoader());
      when(cache.getCacheManager()).thenReturn(cm);
      when(cache.getName()).thenReturn(cacheName);
      when(cache.getAdvancedCache()).thenReturn(cache);
      when(cache.getComponentRegistry()).thenReturn(registry);
      when(cache.getStatus()).thenReturn(ComponentStatus.RUNNING);
      when(cache.getCacheConfiguration()).thenReturn(configuration);
      return cache;
   }
}
