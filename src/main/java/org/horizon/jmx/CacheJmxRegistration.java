/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.horizon.jmx;

import org.horizon.AdvancedCache;
import org.horizon.CacheException;
import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.factories.AbstractComponentRegistry;
import org.horizon.factories.GlobalComponentRegistry;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.annotations.Start;
import org.horizon.factories.annotations.Stop;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;
import org.horizon.util.Util;

import javax.management.MBeanServer;
import java.util.Set;

/**
 * If {@link Configuration#isExposeJmxStatistics()} is true, then class will register all the MBeans from cache local's
 * ConfigurationRegistry to the MBean server.
 *
 * @author Mircea.Markus@jboss.com
 * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
 * @since 4.0
 */
@NonVolatile
public class CacheJmxRegistration {
   private static final Log log = LogFactory.getLog(CacheJmxRegistration.class);

   private AdvancedCache cache;

   @Inject
   public void initialize(AdvancedCache cache) {
      this.cache = cache;
   }

   /**
    * Here is where the registration is being performed.
    */
   @Start(priority = 14)
   public void registerToMBeanServer() {
      if (cache == null)
         throw new IllegalStateException("The cache should had been injected before a call to this method");
      Configuration config = cache.getConfiguration();
      if (config.isExposeJmxStatistics()) {
         ComponentsJmxRegistration registrator = buildRegistrator();
         registrator.registerMBeans();
         log.info("MBeans were successfully registered to the platform mbean server.");
      }
   }

   /**
    * Unregister when the cache is being stoped.
    */
   @Stop
   public void unregisterMBeans() {
      //this method might get called several times.
      // After the first call the cache will become null, so we guard this
      if (cache == null) return;
      Configuration config = cache.getConfiguration();
      if (config.isExposeJmxStatistics()) {
         ComponentsJmxRegistration componentsJmxRegistration = buildRegistrator();
         componentsJmxRegistration.unregisterMBeans();
         log.trace("MBeans were successfully unregistered from the mbean server.");
      }
      cache = null;
   }

   private ComponentsJmxRegistration buildRegistrator() {
      Set<AbstractComponentRegistry.Component> components = cache.getComponentRegistry().getRegisteredComponents();
      GlobalConfiguration configuration = cache.getConfiguration().getGlobalConfiguration();
      MBeanServer beanServer = getMBeanServer(configuration);
      ComponentsJmxRegistration registrator = new ComponentsJmxRegistration(beanServer, components, getGroupName());
      updateDomain(registrator, cache.getAdvancedCache().getComponentRegistry().getGlobalComponentRegistry(), beanServer);
      return registrator;
   }

   static void updateDomain(ComponentsJmxRegistration registrator, GlobalComponentRegistry componentRegistry, MBeanServer mBeanServer) {
      GlobalConfiguration globalConfiguration = componentRegistry.getComponent(GlobalConfiguration.class);
      String componentName = CacheJmxRegistration.class.getName() + "_jmxDomain";
      String jmxDomain = componentRegistry.getComponent(String.class, componentName);
      if (jmxDomain == null) {
         jmxDomain = getJmxDomain(globalConfiguration.getJmxDomain(), mBeanServer);
         componentRegistry.registerComponent(jmxDomain, componentName);
      }
      registrator.setJmxDomain(jmxDomain);
   }

   private static String getJmxDomain(String jmxDomain, MBeanServer mBeanServer) {
      String[] registeredDomains = mBeanServer.getDomains();
      int index = 2;
      String finalName = jmxDomain;
      boolean done = false;
      while (!done) {
         done = true;
         for (String domain : registeredDomains) {
            if (domain.equals(finalName)) {
               finalName = jmxDomain + index++;
               done = false;
               break;
            }
         }
      }
      return finalName;
   }

   static MBeanServer getMBeanServer(GlobalConfiguration configuration) {
      String serverLookup = configuration.getMBeanServerLookup();
      try {
         MBeanServerLookup lookup = (MBeanServerLookup) Util.getInstance(serverLookup);
         return lookup.getMBeanServer();
      } catch (Exception e) {
         log.error("Could not instantiate MBeanServerLookup('" + serverLookup + "')", e);
         throw new CacheException(e);
      }
   }

   private String getGroupName() {
      return cache.getName() + "(" + cache.getConfiguration().getCacheModeString().toLowerCase() + ")";
   }
}
