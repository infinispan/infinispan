package org.infinispan.commons.jmx;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

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
