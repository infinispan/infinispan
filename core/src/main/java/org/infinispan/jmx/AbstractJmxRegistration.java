package org.infinispan.jmx;

import java.util.ArrayList;
import java.util.Collection;
import javax.management.MBeanServer;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.components.ComponentMetadata;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Parent class for top level JMX component registration.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractJmxRegistration {
   private static final Log log = LogFactory.getLog(AbstractJmxRegistration.class);

   protected ComponentsJmxRegistration registrar;
   @Inject GlobalConfiguration globalConfig;
   @Inject BasicComponentRegistry basicComponentRegistry;
   @Inject ComponentMetadataRepo componentMetadataRepo;

   String jmxDomain;
   MBeanServer mBeanServer;

   protected abstract ComponentsJmxRegistration buildRegistrar();

   protected void initMBeanServer(GlobalConfiguration globalConfig) {
      try {
         MBeanServerLookup lookup = globalConfig.globalJmxStatistics().mbeanServerLookup();
         if (lookup != null) {
            mBeanServer = JmxUtil.lookupMBeanServer(lookup, globalConfig.globalJmxStatistics().properties());
         }
      } catch (Exception e) {
         log.debug("Ignoring exception in MBean server lookup", e);
      }
      if (mBeanServer != null) {
         registrar = buildRegistrar();
      }
   }

   protected void unregisterMBeans(Collection<ResourceDMBean> resourceDMBeans) {
      if (mBeanServer != null) {
         ComponentsJmxRegistration registrar = buildRegistrar();
         registrar.unregisterMBeans(resourceDMBeans);
      }
   }

   protected Collection<ResourceDMBean> getResourceDMBeansFromComponents(Collection<ComponentRef<?>> components) {
      Collection<ResourceDMBean> resourceDMBeans = new ArrayList<>(components.size());
      for (ComponentRef<?> component : components) {
         Object instance = component.wired();

         ResourceDMBean resourceDMBean = getResourceDMBean(instance, component.getName());
         if (resourceDMBean != null) {
            resourceDMBeans.add(resourceDMBean);
         }
      }
      return resourceDMBeans;
   }

   protected ResourceDMBean getResourceDMBean(Object instance) {
      return getResourceDMBean(instance, null);
   }

   protected ResourceDMBean getResourceDMBean(Object instance, String componentName) {
      ComponentMetadata md = instance != null ?
                             componentMetadataRepo.getComponentMetadata(instance.getClass()) : null;
      if (md == null || !md.isManageable())
         return null;

      ResourceDMBean resourceDMBean;
      try {
         resourceDMBean = new ResourceDMBean(instance, md.toManageableComponentMetadata(), componentName);
      } catch (NoSuchFieldException | ClassNotFoundException e) {
         throw new CacheConfigurationException(e);
      }
      return resourceDMBean;
   }
}
