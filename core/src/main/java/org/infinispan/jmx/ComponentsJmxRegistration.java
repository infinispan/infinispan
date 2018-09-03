package org.infinispan.jmx;

import java.util.Collection;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.commons.jmx.JmxUtil;

/**
 * Registers a set of components on an MBean server.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class ComponentsJmxRegistration {

   private static final Log log = LogFactory.getLog(ComponentsJmxRegistration.class);

   private MBeanServer mBeanServer;

   private String jmxDomain;
   private String groupName;

   public static String COMPONENT_KEY = "component";
   public static String NAME_KEY = "name";

   /**
    * C-tor.
    *
    * @param mBeanServer the server where mbeans are being registered
    * @param groupName   name of jmx group name
    * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
    * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/management/mxbeans.html#mbean_server">platform
    *      MBeanServer</a>
    */
   public ComponentsJmxRegistration(MBeanServer mBeanServer, String groupName) {
      this.mBeanServer = mBeanServer;
      this.groupName = groupName;
   }

   public void setJmxDomain(String jmxDomain) {
      this.jmxDomain = jmxDomain;
   }

   /**
    * Performs the MBean registration.
    * @param resourceDMBeans
    */
   public void registerMBeans(Collection<ResourceDMBean> resourceDMBeans) throws CacheException {
      try {
         for (ResourceDMBean resource : resourceDMBeans)
            JmxUtil.registerMBean(resource, getObjectName(resource), mBeanServer);
      }
      catch (Exception e) {
         throw new CacheException("Failure while registering mbeans", e);
      }
   }

   /**
    * Unregisters all the MBeans registered through {@link #registerMBeans(Collection)}.
    * @param resourceDMBeans
    */
   public void unregisterMBeans(Collection<ResourceDMBean> resourceDMBeans) throws CacheException {
      log.trace("Unregistering jmx resources..");
      try {
         for (ResourceDMBean resource : resourceDMBeans) {
            JmxUtil.unregisterMBean(getObjectName(resource), mBeanServer);
         }
      }
      catch (Exception e) {
         throw new CacheException("Failure while unregistering mbeans", e);
      }
   }

   private ObjectName getObjectName(ResourceDMBean resource) throws Exception {
      return getObjectName(resource.getObjectName());
   }

   protected ObjectName getObjectName(String resourceName) throws Exception {
      return new ObjectName(getObjectName(jmxDomain, groupName, resourceName));
   }

   public static String getObjectName(String jmxDomain, String groupName, String resourceName) {
      return jmxDomain + ":" + groupName + "," + COMPONENT_KEY + "=" + resourceName;
   }
}
