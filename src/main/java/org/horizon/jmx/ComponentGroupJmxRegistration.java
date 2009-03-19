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

import org.horizon.CacheException;
import org.horizon.factories.AbstractComponentRegistry;
import org.horizon.factories.ComponentRegistry;
import org.horizon.logging.Log;
import org.horizon.logging.LogFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Registers all the <b>MBean</b>s from an <b>Cache</b> instance to a <b>MBeanServer</b>. It iterates over all the
 * components within <b>ComponentRegistry</b> and registers all the components annotated with <b>ManagedAttribute</b>,
 * <b>ManagedOperation</b> or/and <b>MBean</b>. If no <b>MBean</b> server is provided, then the {@link
 * java.lang.management.ManagementFactory#getPlatformMBeanServer()} is being used. <p/> It is immutable: both cache
 * instance and MBeanServer are being passed as arguments to the constructor. <p /> <p> Note that by default object
 * names used are prefixed with <tt>jboss.cache:service=Horizon</tt>.  While this format works for and is consistent
 * with JBoss AS and the JMX console, it has been known to cause problems with other JMX servers such as Websphere.  To
 * work around this, you can provide the following VM system property to override this prefix with a prefix of your
 * choice: <tt><b>-Dhorizon.jmx.prefix=Horizon</b></tt> </p>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ComponentGroupJmxRegistration {

   private static final Log log = LogFactory.getLog(ComponentGroupJmxRegistration.class);

   private MBeanServer mBeanServer;

   private String jmxDomain = "horizon:";
   private String groupName;

   private Set<AbstractComponentRegistry.Component> components;

   public static final String CACHE_NAME_KEY = "cache-name";
   public static final String JMX_RESOURCE_KEY = "jmx-resource";

   /**
    * C-tor.
    *
    * @param mBeanServer    the server where mbeans are being registered
    * @param cache          cache that needs to be monitored
    * @param objectNameBase path in the MBeanServer where to register cache MBeans
    * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
    * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/management/mxbeans.html#mbean_server">platform
    *      MBeanServer</a>
    */
   public ComponentGroupJmxRegistration(MBeanServer mBeanServer, Set<AbstractComponentRegistry.Component> components, String groupName) {
      this.mBeanServer = mBeanServer;
      this.components = components;
      this.groupName = groupName;
   }

   public void setJmxDomain(String jmxDomain) {
      this.jmxDomain = jmxDomain;
   }

   /**
    * Performs the MBean registration.
    */
   public void registerMBeans() throws CacheException {
      try {
         List<ResourceDMBean> resourceDMBeans = getResourceDMBeansFromComponents();
         for (ResourceDMBean resource : resourceDMBeans) {
            String resourceName = resource.getObjectName();
            ObjectName objectName = new ObjectName(getObjectName(resourceName));
            if (!mBeanServer.isRegistered(objectName)) {
               try {
                  mBeanServer.registerMBean(resource, objectName);
               } catch (InstanceAlreadyExistsException e) {
                  //this might happen if multiple instances are trying to concurrently register same objectName
                  log.info("Could not register object with name:" + objectName + "(" + e.getMessage() + ")");
               }
            } else {
               if (log.isInfoEnabled())
                  log.info("Could not register object with name: " + objectName);
            }
         }
      }
      catch (Exception e) {
         throw new CacheException("Failure while registering mbeans", e);
      }
   }

   /**
    * Unregisters all the MBeans registered through {@link #registerMBeans()}.
    */
   public void unregisterCacheMBeans() throws CacheException {
      log.trace("Unregistering jmx resources..");
      try {
         List<ResourceDMBean> resourceDMBeans = getResourceDMBeansFromComponents();
         for (ResourceDMBean resource : resourceDMBeans) {
            String resourceName = resource.getObject().getClass().getSimpleName();
            ObjectName objectName = new ObjectName(getObjectName(resourceName));
            if (mBeanServer.isRegistered(objectName)) {
               mBeanServer.unregisterMBean(objectName);
            }
         }
      }
      catch (Exception e) {
         throw new CacheException("Failure while unregistering mbeans", e);
      }
   }

   private List<ResourceDMBean> getResourceDMBeansFromComponents() {
      List<ResourceDMBean> resourceDMBeans = new ArrayList<ResourceDMBean>();
      for (ComponentRegistry.Component component : components) {
         ResourceDMBean resourceDMBean = new ResourceDMBean(component.getInstance());
         if (resourceDMBean.isManagedResource()) {
            resourceDMBeans.add(resourceDMBean);
         }
      }
      return resourceDMBeans;
   }

   public String getObjectName(String resourceName) {
      return getObjectName(jmxDomain, groupName, resourceName);
   }

   public static String getObjectName(String jmxDomain, String groupName, String resourceName) {
      return jmxDomain + CACHE_NAME_KEY + "=" + groupName + "," + JMX_RESOURCE_KEY + "=" + resourceName;
   }
}
