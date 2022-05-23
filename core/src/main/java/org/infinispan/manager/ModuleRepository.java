package org.infinispan.manager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.impl.ComponentAccessor;
import org.infinispan.factories.impl.DynamicModuleMetadataProvider;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.impl.ModuleMetadataBuilder;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.CyclicDependencyException;
import org.infinispan.util.DependencyGraph;

/**
 * Store for component and module information.
 * <p>
 * <b>NOTE:</b> Not public API: It exists in package {@code org.infinispan.manager}
 * so that only {@code DefaultCacheManager} can instantiate it.
 *
 * @api.private
 * @author Dan Berindei
 * @since 10.0
 */
public final class ModuleRepository {
   private final List<ModuleLifecycle> moduleLifecycles;
   private final Map<String, ComponentAccessor<?>> components;
   private final Map<String, String> factoryNames;
   private final Map<String, MBeanMetadata> mbeans;

   private ModuleRepository(List<ModuleLifecycle> moduleLifecycles,
                    Map<String, ComponentAccessor<?>> components,
                    Map<String, String> factoryNames,
                    Map<String, MBeanMetadata> mbeans) {
      this.moduleLifecycles = moduleLifecycles;
      this.components = components;
      this.factoryNames = factoryNames;
      this.mbeans = mbeans;
   }

   public static ModuleRepository newModuleRepository(ClassLoader classLoader, GlobalConfiguration globalConfiguration) {
      return new Builder(classLoader, globalConfiguration).build();
   }

   public ComponentAccessor<Object> getComponentAccessor(String componentClassName) {
      return (ComponentAccessor<Object>) components.get(componentClassName);
   }

   public String getFactoryName(String componentName) {
      return factoryNames.get(componentName);
   }

   public MBeanMetadata getMBeanMetadata(String componentName) {
      return mbeans.get(componentName);
   }

   public Collection<ModuleLifecycle> getModuleLifecycles() {
      return moduleLifecycles;
   }

   private static final class Builder implements ModuleMetadataBuilder.ModuleBuilder {
      private final List<ModuleLifecycle> moduleLifecycles = new ArrayList<>();
      private final Map<String, ComponentAccessor<?>> components = new HashMap<>();
      private final Map<String, String> factoryNames = new HashMap<>();
      private final Map<String, MBeanMetadata> mbeans = new HashMap<>();

      private Builder(ClassLoader classLoader, GlobalConfiguration globalConfiguration) {
         Collection<ModuleMetadataBuilder> serviceLoader =
            ServiceFinder.load(ModuleMetadataBuilder.class, ModuleRepository.class.getClassLoader(), classLoader);
         Map<String, ModuleMetadataBuilder> modulesMap = new HashMap<>();
         for (ModuleMetadataBuilder module : serviceLoader) {
            ModuleMetadataBuilder existing = modulesMap.put(module.getModuleName(), module);
            if (existing != null) {
               throw new IllegalStateException("Multiple modules registered with name " + module.getModuleName());
            }
         }
         List<ModuleMetadataBuilder> modules = sortModuleDependencies(modulesMap);
         for (ModuleMetadataBuilder module : modules) {
            // register static metadata
            module.registerMetadata(this);

            ModuleLifecycle moduleLifecycle = module.newModuleLifecycle();
            moduleLifecycles.add(moduleLifecycle);

            // register dynamic metadata
            if (moduleLifecycle instanceof DynamicModuleMetadataProvider) {
               ((DynamicModuleMetadataProvider) moduleLifecycle).registerDynamicMetadata(this, globalConfiguration);
            }
         }
      }

      private ModuleRepository build() {
         return new ModuleRepository(moduleLifecycles, components, factoryNames, mbeans);
      }

      private static List<ModuleMetadataBuilder> sortModuleDependencies(Map<String, ModuleMetadataBuilder> modulesMap) {
         DependencyGraph<ModuleMetadataBuilder> dependencyGraph = new DependencyGraph<>();
         for (ModuleMetadataBuilder module : modulesMap.values()) {
            for (String dependencyName : module.getRequiredDependencies()) {
               ModuleMetadataBuilder dependency = modulesMap.get(dependencyName);
               if (dependency == null) {
                  throw new CacheConfigurationException("Missing required dependency: Module '"
                        + module.getModuleName() + "' requires '" + dependencyName + "'");
               }
               dependencyGraph.addDependency(dependency, module);
            }
            for (String dependencyName : module.getOptionalDependencies()) {
               ModuleMetadataBuilder dependency = modulesMap.get(dependencyName);
               if (dependency != null) {
                  dependencyGraph.addDependency(dependency, module);
               }
            }
         }
         try {
            List<ModuleMetadataBuilder> sortedBuilders = dependencyGraph.topologicalSort();
            for (ModuleMetadataBuilder module : modulesMap.values()) {
               if (!sortedBuilders.contains(module)) {
                  sortedBuilders.add(module);
               }
            }
            return sortedBuilders;
         } catch (CyclicDependencyException e) {
            throw new CacheConfigurationException(e);
         }
      }

      @Override
      public void registerComponentAccessor(String componentClassName, List<String> factoryComponentNames,
                                            ComponentAccessor<?> accessor) {
         components.put(componentClassName, accessor);
         for (String factoryComponentName : factoryComponentNames) {
            factoryNames.put(factoryComponentName, componentClassName);
         }
      }

      @Override
      public void registerMBeanMetadata(String componentClassName, MBeanMetadata mBeanMetadata) {
         mbeans.put(componentClassName, mBeanMetadata);
      }

      @Override
      public String getFactoryName(String componentName) {
         return factoryNames.get(componentName);
      }
   }
}
