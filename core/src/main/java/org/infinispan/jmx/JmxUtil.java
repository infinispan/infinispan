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
import org.infinispan.configuration.global.GlobalConfiguration;
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

   /**
    * Looks up the {@link javax.management.MBeanServer} instance based on the
    * configuration parameters.
    *
    * @param cfg configuration instance indicating how to lookup
    *            the {@link javax.management.MBeanServer}
    * @return an instance of {@link javax.management.MBeanServer}
    */
   public static MBeanServer lookupMBeanServer(GlobalConfiguration cfg) {
      MBeanServerLookup lookup = cfg.globalJmxStatistics().mbeanServerLookup();
      return lookup.getMBeanServer(cfg.globalJmxStatistics().properties());
   }

   /**
    * Build the JMX domain name.
    *
    * @param cfg configuration instance containig rules on JMX domains allowed
    * @param mBeanServer the {@link javax.management.MBeanServer} where to
    *                    check whether the JMX domain is allowed or not.
    * @param groupName String containing the group name for the JMX MBean
    * @return A string that combines the allowed JMX domain and the group name
    */
   public static String buildJmxDomain(GlobalConfiguration cfg, MBeanServer mBeanServer, String groupName) {
      String jmxDomain = findJmxDomain(cfg.globalJmxStatistics().domain(), mBeanServer, groupName);
      String configJmxDomain = cfg.globalJmxStatistics().domain();
      if (!jmxDomain.equals(configJmxDomain) && !cfg.globalJmxStatistics().allowDuplicateDomains()) {
         log.cacheManagerAlreadyRegistered(configJmxDomain);
         throw new JmxDomainConflictException(String.format(
               "Domain already registered %s when trying to register: %s", configJmxDomain, groupName));
      }
      return jmxDomain;
   }

   /**
    * Register the given dynamic JMX MBean.
    *
    * @param mbean Dynamic MBean to register
    * @param objectName {@link javax.management.ObjectName} under which to register the MBean.
    * @param mBeanServer {@link javax.management.MBeanServer} where to store the MBean.
    * @throws Exception If registration could not be completed.
    */
   public static void registerMBean(Object mbean, ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      if (!mBeanServer.isRegistered(objectName)) {
         try {
            mBeanServer.registerMBean(mbean, objectName);
            log.tracef("Registered %s under %s", mbean, objectName);
         } catch (InstanceAlreadyExistsException e) {
            //this might happen if multiple instances are trying to concurrently register same objectName
            log.couldNotRegisterObjectName(objectName, e);
         }
      } else {
         log.debugf("Object name %s already registered", objectName);
      }
   }

   /**
    * Unregister the MBean located under the given {@link javax.management.ObjectName}
    *
    * @param objectName {@link javax.management.ObjectName} where the MBean is registered
    * @param mBeanServer {@link javax.management.MBeanServer} from which to unregister the MBean.
    * @throws Exception If unregistration could not be completed.
    */
   public static void unregisterMBean(ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      if (mBeanServer.isRegistered(objectName)) {
         mBeanServer.unregisterMBean(objectName);
         log.tracef("Unregistered %s", objectName);
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
