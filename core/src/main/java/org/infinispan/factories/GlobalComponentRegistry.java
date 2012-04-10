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

import org.infinispan.CacheException;
import org.infinispan.Version;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerStartupException;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifierImpl;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.ModuleProperties;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior.DEFAULT;
import static org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior.REGISTER;

/**
 * A global component registry where shared components are stored.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class GlobalComponentRegistry extends AbstractComponentRegistry {

   private static final Log log = LogFactory.getLog(GlobalComponentRegistry.class);
   private static boolean versionLogged = false;
   /**
    * Hook to shut down the cache when the JVM exits.
    */
   private Thread shutdownHook;
   /**
    * A flag that the shutdown hook sets before calling cache.stop().  Allows stop() to identify if it has been called
    * from a shutdown hook.
    */
   private boolean invokedFromShutdownHook;

   private final GlobalConfiguration globalConfiguration;

   /**
    * Tracking set of created caches in order to make it easy to remove a cache on remote nodes.
    */
   private final Set<String> createdCaches;

   private final ModuleProperties moduleProperties = new ModuleProperties();
   final List<ModuleLifecycle> moduleLifecycles;

   final Map<String, ComponentRegistry> namedComponents = new HashMap<String, ComponentRegistry>(4);

   /**
    * Creates an instance of the component registry.  The configuration passed in is automatically registered.
    *
    * @param configuration configuration with which this is created
    */
   public GlobalComponentRegistry(GlobalConfiguration configuration,
                                  EmbeddedCacheManager cacheManager,
                                  Set<String> createdCaches) {
      super(configuration.getClassLoader()); // registers the default classloader
      moduleLifecycles = moduleProperties.resolveModuleLifecycles(defaultClassLoader);

      // Load up the component metadata
      ComponentMetadataRepo.initialize(moduleProperties.getModuleMetadataFiles(defaultClassLoader), defaultClassLoader);

      try {
         // this order is important ...
         globalConfiguration = configuration;

         registerComponent(this, GlobalComponentRegistry.class);
         registerComponent(cacheManager, EmbeddedCacheManager.class);
         registerComponent(configuration, GlobalConfiguration.class);
         registerComponent(new CacheManagerJmxRegistration(), CacheManagerJmxRegistration.class);
         registerComponent(new CacheManagerNotifierImpl(), CacheManagerNotifier.class);

         moduleProperties.loadModuleCommandHandlers(configuration.getClassLoader());
         Map<Byte, ModuleCommandFactory> factories = moduleProperties.moduleCommandFactories();
         if (factories != null && !factories.isEmpty())
            registerNonVolatileComponent(factories, KnownComponentNames.MODULE_COMMAND_FACTORIES);
         else
            registerNonVolatileComponent(Collections.<Object, Object>emptyMap(), KnownComponentNames.MODULE_COMMAND_FACTORIES);
         this.createdCaches = createdCaches;

         // This is necessary to make sure the transport has been started and is available to other components that
         // may need it.  This is a messy approach though - a proper fix will be in ISPN-1698
         getOrCreateComponent(Transport.class);

      } catch (Exception e) {
         throw new CacheException("Unable to construct a GlobalComponentRegistry!", e);
      }
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected void removeShutdownHook() {
      // if this is called from a source other than the shutdown hook, de-register the shutdown hook.
      if (!invokedFromShutdownHook && shutdownHook != null) Runtime.getRuntime().removeShutdownHook(shutdownHook);
   }

   @Override
   protected void addShutdownHook() {
      ArrayList<MBeanServer> al = MBeanServerFactory.findMBeanServer(null);
      boolean registerShutdownHook = (globalConfiguration.getShutdownHookBehavior() == DEFAULT && al.isEmpty())
            || globalConfiguration.getShutdownHookBehavior() == REGISTER;

      if (registerShutdownHook) {
         log.tracef("Registering a shutdown hook.  Configured behavior = %s", globalConfiguration.getShutdownHookBehavior());
         shutdownHook = new Thread() {
            @Override
            public void run() {
               try {
                  invokedFromShutdownHook = true;
                  GlobalComponentRegistry.this.stop();
               } finally {
                  invokedFromShutdownHook = false;
               }
            }
         };

         Runtime.getRuntime().addShutdownHook(shutdownHook);
      } else {

         log.tracef("Not registering a shutdown hook.  Configured behavior = %s", globalConfiguration.getShutdownHookBehavior());
      }
   }

   public final ComponentRegistry getNamedComponentRegistry(String name) {
      return namedComponents.get(name);
   }

   public final void registerNamedComponentRegistry(ComponentRegistry componentRegistry, String name) {
      namedComponents.put(name, componentRegistry);
   }

   public final void unregisterNamedComponentRegistry(String name) {
      namedComponents.remove(name);
   }

   public final void rewireNamedRegistries() {
      for (ComponentRegistry cr : namedComponents.values())
         cr.rewire();
   }

   public Map<Byte,ModuleCommandInitializer> getModuleCommandInitializers() {
      return moduleProperties.moduleCommandInitializers();
   }

   @Override
   public void start() {
      try {
         boolean needToNotify = state != ComponentStatus.RUNNING && state != ComponentStatus.INITIALIZING;
         if (needToNotify) {
            for (ModuleLifecycle l : moduleLifecycles) {
               l.cacheManagerStarting(this, globalConfiguration);
            }
         }
         super.start();

         if (!versionLogged) {
            log.version(Version.printVersion());
            versionLogged = true;
         }

         if (needToNotify && state == ComponentStatus.RUNNING) {
            for (ModuleLifecycle l : moduleLifecycles) {
               l.cacheManagerStarted(this);
            }
         }
      } catch (RuntimeException rte) {
         try {
            resetVolatileComponents();
            rewire();
         } catch (Exception e) {
            if (log.isDebugEnabled())
               log.warn("Unable to reset GlobalComponentRegistry after a failed restart!", e);
            else
               log.warn("Unable to reset GlobalComponentRegistry after a failed restart due to an exception of type " + e.getClass().getSimpleName() + " with message " + e.getMessage() +". Use DEBUG level logging for full exception details.");
         }
         throw new EmbeddedCacheManagerStartupException(rte);
      }
   }

   @Override
   public void stop() {
      boolean needToNotify = state == ComponentStatus.RUNNING || state == ComponentStatus.INITIALIZING;
      if (needToNotify) {
         for (ModuleLifecycle l : moduleLifecycles) {
            l.cacheManagerStopping(this);
         }
      }

      super.stop();

      if (state == ComponentStatus.TERMINATED && needToNotify) {
         for (ModuleLifecycle l : moduleLifecycles) {
            l.cacheManagerStopped(this);
         }
      }
   }

   public final GlobalConfiguration getGlobalConfiguration() {
      return globalConfiguration;
   }

   /**
    * Removes a cache with the given name, returning true if the cache was removed.
    */
   public boolean removeCache(String cacheName) {
      return createdCaches.remove(cacheName);
   }

   public ModuleProperties getModuleProperties() {
      return moduleProperties;
   }
}
