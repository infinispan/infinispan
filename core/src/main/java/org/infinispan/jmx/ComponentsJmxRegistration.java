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
package org.infinispan.jmx;

import org.infinispan.CacheDelegate;
import org.infinispan.CacheException;
import org.infinispan.factories.AbstractComponentRegistry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

   private Set<AbstractComponentRegistry.Component> components;

   public static String COMPONENT_KEY = "component";
   public static String NAME_KEY = "name";   

   /**
    * C-tor.
    *
    * @param mBeanServer the server where mbeans are being registered
    * @param components  components
    * @param groupName   name of jmx group name
    * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
    * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/management/mxbeans.html#mbean_server">platform
    *      MBeanServer</a>
    */
   public ComponentsJmxRegistration(MBeanServer mBeanServer, Set<AbstractComponentRegistry.Component> components, String groupName) {
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
         boolean trace = log.isTraceEnabled();
         for (ResourceDMBean resource : resourceDMBeans) {
            ObjectName objectName = getObjectName(resource);
            if (!mBeanServer.isRegistered(objectName)) {
               try {
                  mBeanServer.registerMBean(resource, objectName);
                  if (trace) log.trace(String.format("Registered %s under %s", resource, objectName));
               } catch (InstanceAlreadyExistsException e) {
                  //this might happen if multiple instances are trying to concurrently register same objectName
                  log.info("Could not register object with name:" + objectName + "(" + e.getMessage() + ")");
               }
            } else {
               if (log.isDebugEnabled())
                  log.debug(String.format("Object name %s already registered", objectName));
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
   public void unregisterMBeans() throws CacheException {
      log.trace("Unregistering jmx resources..");
      try {
         List<ResourceDMBean> resourceDMBeans = getResourceDMBeansFromComponents();
         boolean trace = log.isTraceEnabled();
         for (ResourceDMBean resource : resourceDMBeans) {
            ObjectName objectName = getObjectName(resource);
            if (mBeanServer.isRegistered(objectName)) {
               mBeanServer.unregisterMBean(objectName);
               if (trace) log.trace("Unregistered " + objectName);
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
