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
import org.horizon.config.Configuration;
import org.horizon.config.GlobalConfiguration;
import org.horizon.factories.AbstractComponentRegistry;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.annotations.Start;
import org.horizon.factories.annotations.Stop;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * If {@link Configuration#isExposeManagementStatistics()} is true, then class will register all the MBeans from the
 * ConfigurationRegistry to the pltform MBean server.
 * <p/>
 * Note: to enable platform MBeanServer the following system property should be passet to the Sun JVM:
 * <b>-Dcom.sun.management.jmxremote</b>.
 *
 * @author Mircea.Markus@jboss.com
 * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
 * @since 1.0
 */
@NonVolatile
public class PlatformMBeanServerCacheRegistration {
   private static final Log log = LogFactory.getLog(PlatformMBeanServerCacheRegistration.class);

   private AdvancedCache cache;

   @Inject
   public void initialize(AdvancedCache cache) {
      this.cache = cache;
   }

   /**
    * Here is where the registration is being performed.
    */
   @Start(priority = 14)
   public void registerToPlatformMBeanServer() {
      if (cache == null)
         throw new IllegalStateException("The cache should had been injected before a call to this method");
      Configuration config = cache.getConfiguration();
      if (config.isExposeManagementStatistics()) {
         ComponentGroupJmxRegistration registrator = buildRegistrator();
         registrator.registerMBeans();
         log.info("MBeans were successfully registered to the platform mbean server.");
      }
   }

   private ComponentGroupJmxRegistration buildRegistrator() {
      Set<AbstractComponentRegistry.Component> components = cache.getComponentRegistry().getRegisteredComponents();
      ComponentGroupJmxRegistration registrator = new ComponentGroupJmxRegistration(ManagementFactory.getPlatformMBeanServer(), components, getGroupName());
      GlobalConfiguration globalConfiguration = cache.getConfiguration().getGlobalConfiguration();
      if (globalConfiguration.getJmxDomain() != null) {
         registrator.setJmxDomain(globalConfiguration.getJmxDomain());
      }
      return registrator;
   }

   private String getGroupName() {
      return cache.getName() + "(" + cache.getConfiguration().getCacheModeString().toLowerCase() + ")";
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
      if (config.isExposeManagementStatistics()) {
         ComponentGroupJmxRegistration componentGroupJmxRegistration = buildRegistrator();
         componentGroupJmxRegistration.unregisterCacheMBeans();
         log.trace("MBeans were successfully unregistered from the platform mbean server.");
      }
      cache = null;
   }
}
