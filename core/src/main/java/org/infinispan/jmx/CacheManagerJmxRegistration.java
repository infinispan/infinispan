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
package org.infinispan.jmx;

import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.AbstractComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.Set;

/**
 * Registers all the components from global component registry to the mbean server.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@SurvivesRestarts
public class CacheManagerJmxRegistration extends AbstractJmxRegistration {
   private static final Log log = LogFactory.getLog(CacheManagerJmxRegistration.class);
   public static final String CACHE_MANAGER_JMX_GROUP = "type=CacheManager";
   private GlobalComponentRegistry globalReg;

   @Inject
   public void init(GlobalComponentRegistry registry, GlobalConfiguration configuration) {
      this.globalReg = registry;
      this.globalConfig = configuration;
   }

   /**
    * On start, the mbeans are registered.
    */
   public void start() {
      if (globalConfig.isExposeGlobalJmxStatistics()) {
         registerMBeans(globalReg.getRegisteredComponents(), globalConfig);
      }
   }

   /**
    * On stop, the mbeans are unregistered.
    */
   public void stop() {
      // This method might get called several times.
      // After the first call the cache will become null, so we guard this
      if (globalReg == null) return;
      if (globalConfig.isExposeGlobalJmxStatistics()) {
         unregisterMBeans(globalReg.getRegisteredComponents());
      }
      globalReg = null;
   }

   @Override
   protected ComponentsJmxRegistration buildRegistrar(Set<AbstractComponentRegistry.Component> components) {
      // Quote group name, to handle invalid ObjectName characters      
      String groupName = CACHE_MANAGER_JMX_GROUP
            + "," + ComponentsJmxRegistration.NAME_KEY
            + "=" + ObjectName.quote(globalConfig.getCacheManagerName());
      ComponentsJmxRegistration registrar = new ComponentsJmxRegistration(mBeanServer, components, groupName);
      updateDomain(registrar, mBeanServer, groupName);
      return registrar;
   }

   protected void updateDomain(ComponentsJmxRegistration registrar, MBeanServer mBeanServer, String groupName) {
      if (jmxDomain == null) {
         jmxDomain = JmxUtil.buildJmxDomain(globalConfig, mBeanServer, groupName);
         String configJmxDomain = globalConfig.getJmxDomain();
         if (!jmxDomain.equals(configJmxDomain) && !globalConfig.isAllowDuplicateDomains()) {
            log.cacheManagerAlreadyRegistered(configJmxDomain);
            throw new JmxDomainConflictException(String.format("Domain already registered %s", configJmxDomain));
         }
      }
      registrar.setJmxDomain(jmxDomain);
   }

}
