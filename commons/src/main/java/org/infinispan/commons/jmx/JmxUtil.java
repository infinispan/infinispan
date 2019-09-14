package org.infinispan.commons.jmx;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * Internal class containing JMX related utility methods. Strictly for internal use. This class has just become unstable
 * and might evaporate spontaneously.
 *
 * @author Galder Zamarreño
 * @since 5.0
 * @deprecated Since 10.0. DO NOT USE! Will be moved to an internal package soon.
 */
@Deprecated
public final class JmxUtil {

   private static final Log log = LogFactory.getLog(JmxUtil.class);

   /**
    * Build the JMX domain name. Starts from the given domain and probes it for the existence of any MBeans in the given
    * group. If MBeans exist it appends an increasing numeric suffix (starting from 2) and retries until it finds an
    * unused domain.
    *
    * @param jmxDomain   The JMX domain name
    * @param mBeanServer the {@link MBeanServer} where to check whether the JMX domain is allowed or not.
    * @param groupName   String containing the group name for the JMX MBean
    * @return A string that combines the allowed JMX domain possibly with a unique suffix
    */
   public static String buildJmxDomain(String jmxDomain, MBeanServer mBeanServer, String groupName) {
      if (jmxDomain == null) {
         throw new IllegalArgumentException("jmxDomain cannot be null");
      }
      if (groupName == null) {
         throw new IllegalArgumentException("groupName cannot be null");
      }

      String finalName = jmxDomain;
      int index = 2;
      try {
         while (!SecurityActions.queryNames(new ObjectName(finalName + ':' + groupName + ",*"), null, mBeanServer).isEmpty()) {
            finalName = jmxDomain + index++;
         }
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Failed to check for duplicate JMX domain names", e);
      }

      return finalName;
   }

   /**
    * Registers the JMX MBean.
    *
    * @param mBeanInstance MBean to register
    * @param objectName    {@link ObjectName} under which to register the MBean.
    * @param mBeanServer   {@link MBeanServer} where to store the MBean.
    * @throws Exception If registration could not be completed.
    */
   public static void registerMBean(Object mBeanInstance, ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      try {
         SecurityActions.registerMBean(mBeanInstance, objectName, mBeanServer);
         if (log.isTraceEnabled()) {
            log.tracef("Registered MBean %s under %s", mBeanInstance, objectName);
         }
      } catch (InstanceAlreadyExistsException e) {
         //this might happen if multiple instances are trying to concurrently register same objectName
         log.couldNotRegisterObjectName(objectName, e);
         throw e;
      }
   }

   /**
    * Unregisters the MBean located under the given {@link ObjectName}, if it exists.
    *
    * @param objectName  {@link ObjectName} where the MBean is registered
    * @param mBeanServer {@link MBeanServer} from which to unregister the MBean.
    * @throws Exception If unregistration could not be completed.
    */
   public static void unregisterMBean(ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      if (mBeanServer.isRegistered(objectName)) {
         SecurityActions.unregisterMBean(objectName, mBeanServer);
         if (log.isTraceEnabled()) {
            log.tracef("Unregistered MBean: %s", objectName);
         }
      } else {
         log.debugf("MBean not registered: %s", objectName);
      }
   }
}
