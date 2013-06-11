/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.jcache;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.Set;

/**
 * A convenience class for registering CacheStatisticsMBeans with an
 * MBeanServer.
 *
 * @author Greg Luck
 * @since 1.0
 */
public final class RIMBeanServerRegistrationUtility {

   /**
    * The type of registered Object
    */
   enum ObjectNameType {

      /**
       * Cache Statistics
       */
      STATISTICS("Statistics"),

      /**
       * Cache Configuration
       */
      CONFIGURATION("Configuration");

      private final String objectName;

      ObjectNameType(String objectName) {
         this.objectName = objectName;
      }
   }


   private RIMBeanServerRegistrationUtility() {
      //prevent construction
   }

   /**
    * Utility method for registering CacheStatistics with the platform
    * MBeanServer
    *
    * @param cache the cache to register
    */
   static <K, V> void registerCacheObject(JCache<K, V> cache, ObjectNameType objectNameType) {
      //these can change during runtime, so always look it up
      MBeanServer mBeanServer = cache.getMBeanServer();
      ObjectName registeredObjectName = calculateObjectName(cache, objectNameType);
      try {
         if (objectNameType.equals(ObjectNameType.CONFIGURATION)) {
            if (!isRegistered(cache, objectNameType)) {
               mBeanServer.registerMBean(cache.getCacheMXBean(), registeredObjectName);
            }
         } else if (objectNameType.equals(ObjectNameType.STATISTICS)) {
            if (!isRegistered(cache, objectNameType)) {
               mBeanServer.registerMBean(cache.getCacheStatisticsMXBean(), registeredObjectName);
            }
         }
      } catch (Exception e) {
         throw new CacheException("Error registering cache MXBeans for CacheManager "
               + registeredObjectName + " . Error was " + e.getMessage(), e);
      }
   }

   /**
    * Checks whether an ObjectName is already registered.
    *
    * @throws javax.cache.CacheException - all exceptions are wrapped in
    *                                    CacheException
    */
   static <K, V> boolean isRegistered(JCache<K, V> cache, ObjectNameType objectNameType) {
      Set<ObjectName> registeredObjectNames;
      MBeanServer mBeanServer = cache.getMBeanServer();

      ObjectName objectName = calculateObjectName(cache, objectNameType);
      registeredObjectNames = mBeanServer.queryNames(objectName, null);

      return !registeredObjectNames.isEmpty();
   }

   /**
    * Removes registered CacheStatistics for a Cache
    *
    * @throws javax.cache.CacheException - all exceptions are wrapped in
    *                                    CacheException
    */
   static void unregisterCacheObject(JCache cache, ObjectNameType objectNameType) {
      Set<ObjectName> registeredObjectNames;
      MBeanServer mBeanServer = cache.getMBeanServer();

      ObjectName objectName = calculateObjectName(cache, objectNameType);
      registeredObjectNames = mBeanServer.queryNames(objectName, null);

      //should just be one
      for (ObjectName registeredObjectName : registeredObjectNames) {
         try {
            mBeanServer.unregisterMBean(registeredObjectName);
         } catch (Exception e) {
            throw new CacheException("Error unregistering object instance "
                  + registeredObjectName + " . Error was " + e.getMessage(), e);
         }
      }
   }

   /**
    * Creates an object name using the scheme "javax.cache:type=Cache&lt;Statistics|Configuration&gt;,CacheManager=&lt;cacheManagerName&gt;,name=&lt;cacheName&gt;"
    */
   private static ObjectName calculateObjectName(Cache cache, ObjectNameType objectNameType) {
      String cacheManagerName = mbeanSafe(cache.getCacheManager().getURI().toString());
      String cacheName = mbeanSafe(cache.getName());

      try {
         return new ObjectName("javax.cache:type=Cache" + objectNameType.objectName
               + ",CacheManager=" + cacheManagerName
               + ",Cache=" + cacheName);
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Illegal ObjectName for Management Bean. " +
               "CacheManager=[" + cacheManagerName + "], Cache=[" + cacheName + "]", e);
      }
   }

   /**
    * Filter out invalid ObjectName characters from string.
    *
    * @param string input string
    * @return A valid JMX ObjectName attribute value.
    */
   private static String mbeanSafe(String string) {
      return string == null ? "" : string.replaceAll(",|:|=|\n", ".");
   }

}

