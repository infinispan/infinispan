/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.factories;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheException;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.ScopeDetector;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptyMap;
import static org.infinispan.factories.KnownComponentNames.MODULE_COMMAND_INITIALIZERS;

/**
 * Named cache specific components
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ComponentRegistry extends AbstractComponentRegistry {

   // cached component scopes
   private static final Map<Class, Scopes> componentScopeLookup = new ConcurrentHashMap<Class, Scopes>(1);

   private final GlobalComponentRegistry globalComponents;
   private final String cacheName;
   private static final Log log = LogFactory.getLog(ComponentRegistry.class);
   private CacheManagerNotifier cacheManagerNotifier;

   @Inject
   public void setCacheManagerNotifier(CacheManagerNotifier cacheManagerNotifier) {
      this.cacheManagerNotifier = cacheManagerNotifier;
   }

   /**
    * Creates an instance of the component registry.  The configuration passed in is automatically registered.
    *
    * @param configuration    configuration with which this is created
    * @param cache            cache
    * @param globalComponents Shared Component Registry to delegate to
    */
   public ComponentRegistry(String cacheName, Configuration configuration, AdvancedCache cache,
                            GlobalComponentRegistry globalComponents, ClassLoader defaultClassLoader) {
      super(defaultClassLoader); // registers the default classloader
      try {
         this.cacheName = cacheName;
         if (cacheName == null) throw new ConfigurationException("Cache name cannot be null!");
         if (globalComponents == null) throw new NullPointerException("GlobalComponentRegistry cannot be null!");
         this.globalComponents = globalComponents;

         registerComponent(this, ComponentRegistry.class);
         registerComponent(configuration, Configuration.class);
         registerComponent(new BootstrapFactory(cache, configuration, this), BootstrapFactory.class);

         // register any module-specific command initializers
         // Modules are on the same classloader as Infinispan
         Map<Byte, ModuleCommandInitializer> initializers = globalComponents.getModuleCommandInitializers();
         if (initializers != null && !initializers.isEmpty()) {
            registerNonVolatileComponent(initializers, MODULE_COMMAND_INITIALIZERS);
            for (ModuleCommandInitializer mci: initializers.values()) registerNonVolatileComponent(mci, mci.getClass());
         } else
            registerNonVolatileComponent(emptyMap(), MODULE_COMMAND_INITIALIZERS);
      }
      catch (Exception e) {
         throw new CacheException("Unable to construct a ComponentRegistry!", e);
      }
   }

   protected Log getLog() {
      return log;
   }

   @Override
   public final <T> T getComponent(Class<T> componentType, String name) {
      if (isGlobal(componentType)) {
         return globalComponents.getComponent(componentType, name);
      } else {
         return getLocalComponent(componentType, name);
      }
   }

   @SuppressWarnings("unchecked")
   public final <T> T getLocalComponent(Class<T> componentType, String name) {
      return super.getComponent(componentType, name);
   }

   public final <T> T getLocalComponent(Class<T> componentType) {
      return getLocalComponent(componentType, componentType.getName());
   }

   @Override
   protected final Map<String, Class<? extends AbstractComponentFactory>> getDefaultFactoryMap() {
      // delegate to parent.  No sense maintaining multiple copies of this map.
      return globalComponents.getDefaultFactoryMap();
   }

   @Override
   protected final Component lookupComponent(Class componentClass, String name) {
      if (isGlobal(componentClass)) {
         return globalComponents.lookupComponent(componentClass, name);
      } else {
         return lookupLocalComponent(componentClass, name);
      }
   }

   protected final Component lookupLocalComponent(Class componentClass, String name) {
      return super.lookupComponent(componentClass, name);
   }

   public final GlobalComponentRegistry getGlobalComponentRegistry() {
      return globalComponents;
   }

   @Override
   protected void registerComponentInternal(Object component, String name, boolean nonVolatile) {
      if (isGlobal(component.getClass())) {
         globalComponents.registerComponentInternal(component, name, nonVolatile);
      } else {
         super.registerComponentInternal(component, name, nonVolatile);
      }
   }

   private boolean isGlobal(Class clazz) {
      Scopes componentScope = componentScopeLookup.get(clazz);
      if (componentScope == null) {
         // Because the detectScope call is not protected by a lock, we can end up doing duplicate work
         // However this will happen rarely enough that we can afford to ignore the duplicate work.
         componentScope = ScopeDetector.detectScope(clazz);
         componentScopeLookup.put(clazz, componentScope);
      }

      return componentScope == Scopes.GLOBAL;
   }

   @Override
   public void start() {
      globalComponents.start();
      boolean needToNotify = state != ComponentStatus.RUNNING && state != ComponentStatus.INITIALIZING;

      // set this up *before* starting the components since some components - specifically state transfer - needs to be
      // able to locate this registry via the InboundInvocationHandler
      this.globalComponents.registerNamedComponentRegistry(this, cacheName);

      // Cache starting notification happens earlier in the call stack trace

      super.start();

      if (needToNotify && state == ComponentStatus.RUNNING) {
         for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
            l.cacheStarted(this, cacheName);
         } 
         cacheManagerNotifier.notifyCacheStarted(cacheName);
      }
   }

   void notifyCacheStarting(Configuration configuration) {
      for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
         l.cacheStarting(this, configuration, cacheName);
      }
   }

   @Override
   public void stop() {
      if (state.stopAllowed())globalComponents.unregisterNamedComponentRegistry(cacheName);
      boolean needToNotify = state == ComponentStatus.RUNNING || state == ComponentStatus.INITIALIZING;
      if (needToNotify) {
         for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
            l.cacheStopping(this, cacheName);
         }
      }
      super.stop();
      if (state == ComponentStatus.TERMINATED && needToNotify) {
         for (ModuleLifecycle l : globalComponents.moduleLifecycles) {
            l.cacheStopped(this, cacheName);
         }
         cacheManagerNotifier.notifyCacheStopped(cacheName);
      }
   }

   public String getCacheName() {
      return cacheName;
   }

}
