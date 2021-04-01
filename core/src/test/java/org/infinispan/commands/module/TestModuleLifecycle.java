package org.infinispan.commands.module;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentAccessor;
import org.infinispan.factories.impl.DynamicModuleMetadataProvider;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.factories.impl.WireContext;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ModuleLifecycle;

/**
 * Allow tests to register global or cache components replacing the default ones.
 *
 * @author Dan Berindei
 * @since 9.4
 */
@InfinispanModule(name = "core-tests", requiredModules = "core", optionalModules = {"cloudevents"})
public final class TestModuleLifecycle implements ModuleLifecycle, DynamicModuleMetadataProvider {

   private TestGlobalConfiguration testGlobalConfiguration;

   @Override
   public void registerDynamicMetadata(ModuleMetadataBuilder.ModuleBuilder moduleBuilder, GlobalConfiguration globalConfiguration) {
      testGlobalConfiguration = globalConfiguration.module(TestGlobalConfiguration.class);
      if (testGlobalConfiguration != null) {
         HashMap<String, String> defaultFactoryNames = new HashMap<>();
         List<String> componentNames = new ArrayList<>(testGlobalConfiguration.globalTestComponents().keySet());
         List<String> cacheComponentNames = new ArrayList<>(testGlobalConfiguration.cacheTestComponentNames());
         for (String componentName : cacheComponentNames) {
            defaultFactoryNames.put(componentName, moduleBuilder.getFactoryName(componentName));
         }
         moduleBuilder.registerComponentAccessor(TestGlobalComponentFactory.class.getName(), componentNames,
                                                 new GlobalFactoryComponentAccessor(testGlobalConfiguration));
         moduleBuilder.registerComponentAccessor(TestCacheComponentFactory.class.getName(), cacheComponentNames,
                                                 new CacheFactoryComponentAccessor(testGlobalConfiguration,
                                                                                   defaultFactoryNames));
      }
   }

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      if (testGlobalConfiguration == null)
         return;

      Set<String> componentNames = testGlobalConfiguration.globalTestComponents().keySet();
      for (String componentName : componentNames) {
         assert testGlobalConfiguration.globalTestComponents().get(componentName) == gcr.getComponent(componentName);
      }
   }

   @Override
   public void cacheStarting(ComponentRegistry cr, Configuration configuration, String cacheName) {
      if (testGlobalConfiguration == null)
         return;

      if (testGlobalConfiguration.cacheStartCallback() != null) {
         testGlobalConfiguration.cacheStartCallback().accept(cr);
      }

      Map<String, Object> testCacheComponents = testGlobalConfiguration.cacheTestComponents(cacheName);
      if (testCacheComponents == null)
         return;

      Set<String> componentNames = testCacheComponents.keySet();
      for (String componentName : componentNames) {
         assert testCacheComponents.get(componentName) == cr.getComponent(componentName);
      }
   }

   private static final class TestGlobalComponentFactory implements ComponentFactory {
      private final TestGlobalConfiguration testGlobalConfiguration;

      TestGlobalComponentFactory(TestGlobalConfiguration testGlobalConfiguration) {
         this.testGlobalConfiguration = testGlobalConfiguration;
      }

      @Override
      public Object construct(String componentName) {
         return testGlobalConfiguration.globalTestComponents().get(componentName);
      }
   }

   private static final class TestCacheComponentFactory implements ComponentFactory {
      private final TestGlobalConfiguration testCacheConfiguration;
      private final HashMap<String, String> defaultFactoryNames;

      private String cacheName;
      private BasicComponentRegistry cacheComponentRegistry;

      TestCacheComponentFactory(TestGlobalConfiguration testCacheConfiguration,
                                HashMap<String, String> defaultFactoryNames) {
         this.testCacheConfiguration = testCacheConfiguration;
         this.defaultFactoryNames = defaultFactoryNames;
      }

      @Override
      public Object construct(String componentName) {
         Map<String, Object> testCacheComponents = testCacheConfiguration.cacheTestComponents(cacheName);
         if (testCacheComponents != null) {
            Object testComponent = testCacheComponents.get(componentName);
            if (testComponent != null)
               return testComponent;
         }

         String defaultFactoryName = defaultFactoryNames.get(componentName);
         ComponentFactory defaultFactory =
            cacheComponentRegistry.getComponent(defaultFactoryName, ComponentFactory.class).running();
         return defaultFactory.construct(componentName);
      }
   }

   private static final class GlobalFactoryComponentAccessor extends ComponentAccessor<TestGlobalComponentFactory> {
      private final TestGlobalConfiguration testGlobalConfiguration;

      GlobalFactoryComponentAccessor(TestGlobalConfiguration testGlobalConfiguration) {
         super(TestGlobalComponentFactory.class.getName(), Scopes.GLOBAL.ordinal(), true, null,
               Collections.emptyList());
         this.testGlobalConfiguration = testGlobalConfiguration;
      }

      @Override
      protected TestGlobalComponentFactory newInstance() {
         return new TestGlobalComponentFactory(testGlobalConfiguration);
      }
   }

   private static final class CacheFactoryComponentAccessor extends ComponentAccessor<TestCacheComponentFactory> {
      private final TestGlobalConfiguration testGlobalConfiguration;
      private final HashMap<String, String> defaultFactoryNames;

      CacheFactoryComponentAccessor(TestGlobalConfiguration testGlobalConfiguration,
                                    HashMap<String, String> defaultFactoryNames) {
         super(TestCacheComponentFactory.class.getName(), Scopes.NAMED_CACHE.ordinal(), true, null,
               Collections.emptyList());
         this.testGlobalConfiguration = testGlobalConfiguration;
         this.defaultFactoryNames = defaultFactoryNames;
      }

      @Override
      protected void wire(TestCacheComponentFactory instance, WireContext context, boolean start) {
         instance.cacheComponentRegistry = context.get(BasicComponentRegistry.class.getName(),
                                                       BasicComponentRegistry.class, false);
         instance.cacheName = context.get(KnownComponentNames.CACHE_NAME, String.class, false);
      }

      @Override
      protected TestCacheComponentFactory newInstance() {
         return new TestCacheComponentFactory(testGlobalConfiguration, defaultFactoryNames);
      }
   }
}
