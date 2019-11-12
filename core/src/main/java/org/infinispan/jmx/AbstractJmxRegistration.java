package org.infinispan.jmx;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.impl.ApplicationMetricsRegistry;
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

   @Inject
   ApplicationMetricsRegistry applicationMetricsRegistry;

   volatile MBeanServer mBeanServer;

   String jmxDomain;

   String groupName;

   private List<ResourceDMBean> resourceDMBeans;

   /**
    * The component used for domain reservation.
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
            GlobalJmxStatisticsConfiguration globalJmxConfig = globalConfig.globalJmxStatistics();
            MBeanServerLookup lookup = globalJmxConfig.mbeanServerLookup();
            if (lookup != null) {
               mBeanServer = lookup.getMBeanServer(globalJmxConfig.properties());
            }
         } catch (Exception e) {
            CONTAINER.warn("Ignoring exception in MBean server lookup", e);
         }

         if (mBeanServer != null) {
            // first time!
            groupName = initGroup();

            resourceDMBeans = Collections.synchronizedList(getResourceDMBeansFromComponents());
            Iterator<ResourceDMBean> it = resourceDMBeans.iterator();
            ResourceDMBean first = it.next();

            // register first bean to reserve the domain
            this.jmxDomain = findVirginDomain(mBeanServer, first, globalConfig.globalJmxStatistics());
            this.mBeanServer = mBeanServer;

            // register remaining beans
            try {
               while (it.hasNext()) {
                  ResourceDMBean resourceDMBean = it.next();
                  ObjectName objectName = getObjectName(groupName, resourceDMBean.getMBeanName());
                  register(resourceDMBean, objectName, mBeanServer);
               }
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

   //TODO remove support for allowDuplicateDomains in Infinispan 11. https://issues.jboss.org/browse/ISPN-10900
   private String findVirginDomain(MBeanServer mBeanServer, ResourceDMBean first, GlobalJmxStatisticsConfiguration globalJmxConfig) {
      String jmxDomain = globalJmxConfig.domain();
      int counter = 2;
      while (true) {
         try {
            register(first, getObjectName(jmxDomain, groupName, first.getMBeanName()), mBeanServer);
            break;
         } catch (InstanceAlreadyExistsException | IllegalArgumentException e) {
            if (globalJmxConfig.allowDuplicateDomains()) {
               // add 'unique' suffix and retry
               jmxDomain = globalJmxConfig.domain() + counter++;
            } else {
               throw CONTAINER.jmxMBeanAlreadyRegistered(groupName, globalJmxConfig.domain());
            }
         } catch (Exception e) {
            throw new CacheException("Failure while registering MBeans", e);
         }
      }
      return jmxDomain;
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
      SecurityActions.registerMBean(resourceDMBean, objectName, mBeanServer);
      if (log.isTraceEnabled()) {
         log.tracef("Registered MBean %s under %s", resourceDMBean, objectName);
      }
      if (applicationMetricsRegistry != null) {
         applicationMetricsRegistry.register(resourceDMBean);
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
         SecurityActions.unregisterMBean(objectName, mBeanServer);
         if (log.isTraceEnabled()) {
            log.tracef("Unregistered MBean: %s", objectName);
         }
         if (applicationMetricsRegistry != null) {
            applicationMetricsRegistry.unregister(objectName);
         }
      } else {
         log.debugf("MBean not registered: %s", objectName);
      }
   }
}
