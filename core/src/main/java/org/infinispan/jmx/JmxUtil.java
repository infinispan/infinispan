/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.CacheException;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * Class containing JMX related utility methods.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class JmxUtil {

   private static final Log log = LogFactory.getLog(JmxUtil.class);

   public static MBeanServer lookupMBeanServer(GlobalConfiguration cfg) {
      MBeanServerLookup lookup = cfg.getMBeanServerLookupInstance();
      return lookup.getMBeanServer(cfg.getMBeanServerProperties());
   }

   public static String buildJmxDomain(GlobalConfiguration cfg, MBeanServer mBeanServer, String groupName) {
      String jmxDomain = findJmxDomain(cfg.getJmxDomain(), mBeanServer, groupName);
      String configJmxDomain = cfg.getJmxDomain();
      if (!jmxDomain.equals(configJmxDomain) && !cfg.isAllowDuplicateDomains()) {
         log.cacheManagerAlreadyRegistered(configJmxDomain);
         throw new JmxDomainConflictException(String.format("Domain already registered %s", configJmxDomain));
      }
      return jmxDomain;
   }

   public static void registerMBean(ResourceDMBean dynamicMBean, ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      if (!mBeanServer.isRegistered(objectName)) {
         try {
            mBeanServer.registerMBean(dynamicMBean, objectName);
            if (log.isTraceEnabled()) log.tracef("Registered %s under %s", dynamicMBean, objectName);
         } catch (InstanceAlreadyExistsException e) {
            //this might happen if multiple instances are trying to concurrently register same objectName
            log.couldNotRegisterObjectName(objectName, e);
         }
      } else {
         if (log.isDebugEnabled())
            log.debugf("Object name %s already registered", objectName);
      }
   }

   public static void unregisterMBean(ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      if (mBeanServer.isRegistered(objectName)) {
         mBeanServer.unregisterMBean(objectName);
         if (log.isTraceEnabled()) log.tracef("Unregistered %s", objectName);
      }
   }

   private static String findJmxDomain(String jmxDomain, MBeanServer mBeanServer, String groupName) {
      int index = 2;
      String finalName = jmxDomain;
      boolean done = false;
      while (!done) {
         done = true;
         try {
            ObjectName targetName = new ObjectName(finalName + ':' + groupName + ",*");
            if (mBeanServer.queryNames(targetName, null).size() > 0) {
               finalName = jmxDomain + index++;
               done = false;
            }
         } catch (MalformedObjectNameException e) {
            throw new CacheException("Unable to check for duplicate names", e);
         }
      }

      return finalName;
   }

}
