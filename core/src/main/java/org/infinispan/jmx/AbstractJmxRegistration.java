package org.infinispan.jmx;

import java.util.ArrayList;
import java.util.Collection;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Parent class for JMX component registration.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.NONE)
abstract class AbstractJmxRegistration {

   private static final Log log = LogFactory.getLog(AbstractJmxRegistration.class);

   static final String NAME_KEY = "name";

   private static final String COMPONENT_KEY = "component";

   @Inject
   GlobalConfiguration globalConfig;

   @Inject
   BasicComponentRegistry basicComponentRegistry;

   MBeanServer mBeanServer;

   String jmxDomain;

   protected String groupName;

   /**
    * Looks up the MBean server and initializes domain and group. Overriders must ensure they call super.
    */
   public synchronized void start() {
      // prevent double init of group on eventual restart
      if (groupName == null) {
         groupName = initGroup();
      }

      // prevent double lookup of MBeanServer on eventual restart
      if (mBeanServer == null) {
         try {
            GlobalJmxStatisticsConfiguration globalJmxConfig = globalConfig.globalJmxStatistics();
            MBeanServerLookup lookup = globalJmxConfig.mbeanServerLookup();
            if (lookup != null) {
               mBeanServer = lookup.getMBeanServer(globalJmxConfig.properties());
            }
         } catch (Exception e) {
            log.debug("Ignoring exception in MBean server lookup", e);
         }
      }

      if (mBeanServer != null) {
         // prevent double init of domain on eventual restart
         if (jmxDomain == null) {
            jmxDomain = initDomain();
         }
      }
   }

   /**
    * Subclasses must implement this hook to initialize {@link #jmxDomain}.
    */
   protected abstract String initDomain();

   /**
    * Subclasses must implement this hook to initialize {@link #groupName}.
    */
   protected abstract String initGroup();

   /**
    * Gets the domain name. This should not be called unless JMX is enabled.
    */
   public final String getDomain() {
      if (mBeanServer == null) {
         throw new IllegalStateException("MBean server not initialized");
      }
      return jmxDomain;
   }

   /**
    * Gets the group name. This should not be called unless JMX is enabled.
    */
   public final String getGroupName() {
      if (mBeanServer == null) {
         throw new IllegalStateException("MBean server not initialized");
      }
      return groupName;
   }

   /**
    * Gets the MBean server. This should not be called unless JMX is enabled.
    */
   public final MBeanServer getMBeanServer() {
      if (mBeanServer == null) {
         throw new IllegalStateException("MBean server not initialized");
      }
      return mBeanServer;
   }

   /**
    * Performs the MBean registration (does not track them for automatic unregistration).
    */
   final void internalRegister(Collection<ResourceDMBean> resourceDMBeans) throws CacheException {
      if (mBeanServer == null) {
         throw new IllegalStateException("MBean server not initialized");
      }
      try {
         for (ResourceDMBean resourceDMBean : resourceDMBeans) {
            ObjectName objectName = getObjectName(groupName, resourceDMBean.getMBeanName());
            JmxUtil.registerMBean(resourceDMBean, objectName, mBeanServer);
            resourceDMBean.setObjectName(objectName);
         }
      } catch (Exception e) {
         throw new CacheException("Failure while registering MBeans", e);
      }
   }

   /**
    * Unregisters the given MBeans (untracked).
    */
   final void internalUnregister(Collection<ResourceDMBean> resourceDMBeans) throws CacheException {
      if (mBeanServer == null) {
         throw new IllegalStateException("MBean server not initialized");
      }
      try {
         for (ResourceDMBean resourceDMBean : resourceDMBeans) {
            if (resourceDMBean.getObjectName() != null) {
               JmxUtil.unregisterMBean(resourceDMBean.getObjectName(), mBeanServer);
            }
         }
      } catch (Exception e) {
         throw new CacheException("Failure while unregistering MBeans", e);
      }
   }

   /**
    * Creates an ObjectName based on given group and component name.
    */
   private ObjectName getObjectName(String groupName, String resourceName) throws MalformedObjectNameException {
      if (groupName == null) {
         throw new IllegalArgumentException("groupName cannot be null");
      }
      if (resourceName == null) {
         throw new IllegalArgumentException("resourceName cannot be null");
      }
      return new ObjectName(getDomain() + ":" + groupName + "," + COMPONENT_KEY + "=" + resourceName);
   }

   /**
    * Gathers all components from registry that have MBeanMetadata and creates ResourceDMBeans for them.
    */
   Collection<ResourceDMBean> getResourceDMBeansFromComponents() {
      Collection<ComponentRef<?>> components = basicComponentRegistry.getRegisteredComponents();
      Collection<ResourceDMBean> resourceDMBeans = new ArrayList<>(components.size());
      for (ComponentRef<?> component : components) {
         if (!component.isAlias()) {
            Object instance = component.wired();
            if (instance != null) {
               ResourceDMBean resourceDMBean = getResourceDMBean(instance, component.getName());
               if (resourceDMBean != null) {  // not all components have MBeanMetadata
                  resourceDMBeans.add(resourceDMBean);
               }
            }
         }
      }
      return resourceDMBeans;
   }

   private ResourceDMBean getResourceDMBean(Object instance, String componentName) {
      MBeanMetadata md = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
      return md == null ? null : new ResourceDMBean(instance, md, componentName);
   }

   /**
    * Registers a MBean, but does not track it to perform automatic unregistration on stop. The caller is expected to
    * perform unregistration using the returned ObjectName.
    */
   public ObjectName registerExternalMBean(Object managedComponent, String groupName) throws Exception {
      if (mBeanServer == null) {
         throw new IllegalStateException("MBean server not initialized");
      }

      ResourceDMBean resourceDMBean = getResourceDMBean(managedComponent, null);
      if (resourceDMBean == null) {
         throw new IllegalArgumentException("No MBean metadata found for " + managedComponent.getClass().getName());
      }
      ObjectName objectName = getObjectName(groupName, resourceDMBean.getMBeanName());
      JmxUtil.registerMBean(resourceDMBean, objectName, mBeanServer);
      resourceDMBean.setObjectName(objectName);
      return objectName;
   }

   /**
    * Registers a MBean (and tracks it to perform automatic unregistration on stop). This method should be used for
    * components that are registered after the startup of the component registry and did not get registered
    * automatically.
    */
   public void registerMBean(Object managedComponent) {
      registerMBean(managedComponent, groupName);
   }

   /**
    * Registers a MBean (and tracks it to perform automatic unregistration on stop). This method should be used only for
    * components that are registered after the startup of the component registry and did not get registered
    * automatically.
    */
   public void registerMBean(Object managedComponent, String groupName) {
      try {
         ResourceDMBean resourceDMBean = getResourceDMBean(managedComponent, null);
         if (resourceDMBean == null) {
            throw new IllegalArgumentException("No MBean metadata found for " + managedComponent.getClass().getName());
         }
         ObjectName objectName = getObjectName(groupName, resourceDMBean.getMBeanName());
         JmxUtil.registerMBean(resourceDMBean, objectName, mBeanServer);
         resourceDMBean.setObjectName(objectName);
         trackRegisteredResourceDMBean(resourceDMBean);
      } catch (Exception e) {
         throw new CacheException("Failure while registering MBeans", e);
      }
   }

   /**
    * Add the registered MBean to internal tracking collection to support automatic unregistration on stop.
    */
   protected abstract void trackRegisteredResourceDMBean(ResourceDMBean resourceDMBean);
}
