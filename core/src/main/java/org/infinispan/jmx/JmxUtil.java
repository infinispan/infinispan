package org.infinispan.jmx;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import java.util.Set;

/**
 * Class containing JMX related utility methods.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class JmxUtil {

   private static final Log log = LogFactory.getLog(JmxUtil.class);
   private static final boolean trace = log.isTraceEnabled();

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
         throw log.jmxMBeanAlreadyRegistered(groupName, configJmxDomain);
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
            SecurityActions.registerMBean(mbean, objectName, mBeanServer);
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
         SecurityActions.unregisterMBean(objectName, mBeanServer);
         log.tracef("Unregistered %s", objectName);
      }
   }

   /**
    * Unregister all mbeans whose object names match a given filter.
    *
    * @param filter ObjectName-style formatted filter
    * @param mBeanServer mbean server from which to unregister mbeans
    * @return number of mbeans unregistered
    */
   public static int unregisterMBeans(String filter, MBeanServer mBeanServer) {
      try {
         ObjectName filterObjName = new ObjectName(filter);
         Set<ObjectInstance> mbeans = mBeanServer.queryMBeans(filterObjName, null);
         for (ObjectInstance mbean : mbeans) {
            ObjectName name = mbean.getObjectName();
            if (trace)
               log.trace("Unregistering mbean with name: " + name);
            SecurityActions.unregisterMBean(name, mBeanServer);
         }

         return mbeans.size();
      } catch (Exception e) {
         throw new CacheException(
               "Unable to register mbeans with filter=" + filter, e);
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
            if (SecurityActions.queryNames(targetName, null, mBeanServer).size() > 0) {
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
