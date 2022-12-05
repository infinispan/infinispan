package org.infinispan.jmx;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxConfiguration;
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
abstract class AbstractJmxRegistration implements ObjectNameKeys {

   private static final Log log = LogFactory.getLog(AbstractJmxRegistration.class);

   @Inject
   GlobalConfiguration globalConfig;

   @Inject
   BasicComponentRegistry basicComponentRegistry;

   volatile MBeanServer mBeanServer;

   String groupName;

   private List<ResourceDMBean> resourceDMBeans;

   /**
    * The component to be registered first for domain reservation.
    */
   private final String mainComponent;

   AbstractJmxRegistration(String mainComponent) {
      this.mainComponent = mainComponent;
   }

   /**
    * Looks up the MBean server and initializes domain and group. Overriders must ensure they call super.
    */
   public void start() {
      // prevent double lookup of MBeanServer on eventual restart
      if (mBeanServer == null) {
         MBeanServer mBeanServer = null;
         try {
            GlobalJmxConfiguration jmx = globalConfig.jmx();
            MBeanServerLookup lookup = jmx.mbeanServerLookup();
            if (jmx.enabled() && lookup != null) {
               mBeanServer = lookup.getMBeanServer(jmx.properties());
            }
         } catch (Exception e) {
            CONTAINER.warn("Ignoring exception in MBean server lookup", e);
         }

         if (mBeanServer != null) {
            // first time!
            groupName = initGroup();

            resourceDMBeans = Collections.synchronizedList(getResourceDMBeansFromComponents());
            this.mBeanServer = mBeanServer;

            // register those beans, Jack
            try {
               for (ResourceDMBean resourceDMBean : resourceDMBeans) {
                  ObjectName objectName = getObjectName(groupName, resourceDMBean.getMBeanName());
                  register(resourceDMBean, objectName, mBeanServer);
               }
            } catch (InstanceAlreadyExistsException | IllegalArgumentException e) {
               throw CONTAINER.jmxMBeanAlreadyRegistered(globalConfig.jmx().domain(), e);
            } catch (Exception e) {
               throw new CacheException("Failure while registering MBeans", e);
            }
         }
      } else {
         // restart
         resourceDMBeans = Collections.synchronizedList(getResourceDMBeansFromComponents());
         try {
            for (ResourceDMBean resourceDMBean : resourceDMBeans) {
               ObjectName objectName = getObjectName(groupName, resourceDMBean.getMBeanName());
               register(resourceDMBean, objectName, mBeanServer);
            }
         } catch (Exception e) {
            throw new CacheException("Failure while registering MBeans", e);
         }
      }
   }

   /**
    * Unregisters the MBeans that were registered on start. Overriders must ensure they call super.
    */
   public void stop() {
      if (mBeanServer != null && resourceDMBeans != null) {
         try {
            for (ResourceDMBean resourceDMBean : resourceDMBeans) {
               ObjectName objectName = resourceDMBean.getObjectName();
               if (objectName != null) {
                  unregisterMBean(objectName);
               }
            }
            resourceDMBeans = null;
         } catch (Exception e) {
            throw new CacheException("Failure while unregistering MBeans", e);
         }
      }
   }

   /**
    * Subclasses must implement this hook to initialize {@link #groupName} during start.
    */
   protected abstract String initGroup();

   /**
    * Checks that JMX is effectively enabled.
    */
   public final boolean enabled() {
      return mBeanServer != null;
   }

   /**
    * Gets the domain name. This should not be called unless JMX is enabled.
    */
   public final String getDomain() {
      if (mBeanServer == null) {
         throw new IllegalStateException("MBean server not initialized");
      }
      return globalConfig.jmx().domain();
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
    * Creates an ObjectName based on given domain, group and component name.
    */
   private static ObjectName getObjectName(String domain, String groupName, String resourceName) throws MalformedObjectNameException {
      if (domain == null) {
         throw new IllegalArgumentException("domain cannot be null");
      }
      if (groupName == null) {
         throw new IllegalArgumentException("groupName cannot be null");
      }
      if (resourceName == null) {
         throw new IllegalArgumentException("resourceName cannot be null");
      }
      return new ObjectName(domain + ":" + groupName + "," + COMPONENT + "=" + resourceName);
   }

   /**
    * Creates an ObjectName based on given group and component name.
    */
   private ObjectName getObjectName(String groupName, String resourceName) throws MalformedObjectNameException {
      return getObjectName(getDomain(), groupName, resourceName);
   }

   /**
    * Gathers all components from registry that have MBeanMetadata and creates ResourceDMBeans for them. The first
    * component is always the main component, ie. the cache/cache manager.
    */
   private List<ResourceDMBean> getResourceDMBeansFromComponents() {
      Collection<ComponentRef<?>> components = basicComponentRegistry.getRegisteredComponents();
      List<ResourceDMBean> resourceDMBeans = new ArrayList<>(components.size());
      for (ComponentRef<?> component : components) {
         if (!component.isAlias()) {
            Object instance = component.wired();
            if (instance != null) {
               ResourceDMBean resourceDMBean = getResourceDMBean(instance, component.getName());
               if (resourceDMBean != null) {  // not all components have MBeanMetadata
                  if (mainComponent.equals(resourceDMBean.getMBeanName())) {
                     resourceDMBeans.add(0, resourceDMBean);
                  } else {
                     resourceDMBeans.add(resourceDMBean);
                  }
               }
            }
         }
      }
      if (resourceDMBeans.isEmpty()) {
         throw new IllegalStateException("No MBeans found in component registry!");
      }
      return resourceDMBeans;
   }

   private ResourceDMBean getResourceDMBean(Object instance, String componentName) {
      MBeanMetadata beanMetadata = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
      return beanMetadata == null ? null : new ResourceDMBean(instance, beanMetadata, componentName);
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
      register(resourceDMBean, objectName, mBeanServer);
      return objectName;
   }

   /**
    * Registers a MBean (and tracks it to perform automatic unregistration on stop). This method should be used for
    * components that are registered after the startup of the component registry and did not get registered
    * automatically.
    */
   public void registerMBean(Object managedComponent) throws Exception {
      registerMBean(managedComponent, groupName);
   }

   /**
    * Registers a MBean (and tracks it to perform automatic unregistration on stop). This method should be used only for
    * components that are registered after the startup of the component registry and did not get registered
    * automatically.
    */
   public void registerMBean(Object managedComponent, String groupName) throws Exception {
      if (mBeanServer == null) {
         throw new IllegalStateException("MBean server not initialized");
      }
      ResourceDMBean resourceDMBean = getResourceDMBean(managedComponent, null);
      if (resourceDMBean == null) {
         throw new IllegalArgumentException("No MBean metadata found for " + managedComponent.getClass().getName());
      }
      ObjectName objectName = getObjectName(groupName, resourceDMBean.getMBeanName());
      register(resourceDMBean, objectName, mBeanServer);
      resourceDMBeans.add(resourceDMBean);
   }

   /**
    * Registers the JMX MBean.
    *
    * @param resourceDMBean MBean to register
    * @param objectName     {@link ObjectName} under which to register the MBean.
    * @throws Exception If registration could not be completed.
    */
   private void register(ResourceDMBean resourceDMBean, ObjectName objectName, MBeanServer mBeanServer) throws Exception {
      mBeanServer.registerMBean(resourceDMBean, objectName);
      if (log.isTraceEnabled()) {
         log.tracef("Registered MBean %s under %s", resourceDMBean, objectName);
      }
   }

   /**
    * Unregisters the MBean located under the given {@link ObjectName}, if it exists.
    *
    * @param objectName {@link ObjectName} where the MBean is registered
    * @throws Exception If unregistration could not be completed.
    */
   public void unregisterMBean(ObjectName objectName) throws Exception {
      if (mBeanServer.isRegistered(objectName)) {
         mBeanServer.unregisterMBean(objectName);
         if (log.isTraceEnabled()) {
            log.tracef("Unregistered MBean: %s", objectName);
         }
      } else {
         log.debugf("MBean not registered: %s", objectName);
      }
   }
}
