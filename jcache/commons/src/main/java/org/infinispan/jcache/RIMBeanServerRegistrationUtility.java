package org.infinispan.jcache;

import javax.cache.CacheException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * A convenience class for registering CacheStatisticsMBeans with an MBeanServer.
 *
 * @author Greg Luck
 * @since 1.0
 */
final class RIMBeanServerRegistrationUtility {

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
    * Utility method for registering CacheStatistics with the platform MBeanServer.
    *
    * @param cache the cache to register
    */
   static void registerCacheObject(AbstractJCache<?, ?> cache, ObjectNameType objectNameType) {
      //these can change during runtime, so always look it up
      MBeanServer mBeanServer = cache.getMBeanServer();
      if (mBeanServer != null) {
         Object mBean;
         switch (objectNameType) {
            case CONFIGURATION:
               mBean = cache.getCacheMXBean();
               break;
            case STATISTICS:
               mBean = cache.getCacheStatisticsMXBean();
               break;
            default:
               throw new CacheException("Unrecognized ObjectNameType : " + objectNameType);
         }
         ObjectName objectName = calculateObjectName(cache, objectNameType);
         try {
            if (!mBeanServer.isRegistered(objectName)) {
               SecurityActions.registerMBean(mBean, objectName, mBeanServer);
            }
         } catch (Exception e) {
            throw new CacheException("Error registering cache MXBeans for CacheManager "
                  + objectName + ". Error was " + e.getMessage(), e);
         }
      }
   }

   /**
    * Removes registered CacheStatistics for a Cache.
    *
    * @throws javax.cache.CacheException - all exceptions are wrapped in CacheException
    */
   static void unregisterCacheObject(AbstractJCache<?, ?> cache, ObjectNameType objectNameType) {
      MBeanServer mBeanServer = cache.getMBeanServer();
      if (mBeanServer != null) {
         ObjectName objectName = calculateObjectName(cache, objectNameType);
         try {
            if (mBeanServer.isRegistered(objectName)) {
               SecurityActions.unregisterMBean(objectName, mBeanServer);
            }
         } catch (Exception e) {
            throw new CacheException("Error unregistering MBean " + objectName + ". Error was " + e.getMessage(), e);
         }
      }
   }

   /**
    * Creates an object name using the scheme "javax.cache:type=Cache&lt;Statistics|Configuration&gt;,CacheManager=&lt;cacheManagerName&gt;,name=&lt;cacheName&gt;"
    */
   private static ObjectName calculateObjectName(AbstractJCache<?, ?> cache, ObjectNameType objectNameType) {
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
      return string == null ? "" : string.replaceAll("[,:=\n]", ".");
   }
}
