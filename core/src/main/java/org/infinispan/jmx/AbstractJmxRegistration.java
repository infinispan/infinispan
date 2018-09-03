package org.infinispan.jmx;

import java.util.ArrayList;
import java.util.Collection;
import javax.management.MBeanServer;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.components.ComponentMetadata;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;

/**
 * Parent class for top level JMX component registration.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractJmxRegistration {
   protected ComponentsJmxRegistration registrar;
   @Inject GlobalConfiguration globalConfig;
   @Inject BasicComponentRegistry basicComponentRegistry;
   @Inject ComponentMetadataRepo componentMetadataRepo;

   String jmxDomain;
   MBeanServer mBeanServer;

   protected abstract ComponentsJmxRegistration buildRegistrar();

   protected void initMBeanServer(GlobalConfiguration globalConfig) {
      try {
         mBeanServer = JmxUtil.lookupMBeanServer(globalConfig.globalJmxStatistics().mbeanServerLookup(),
                                                 globalConfig.globalJmxStatistics().properties());
      } catch (Exception e) {
         mBeanServer = null;
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

         ResourceDMBean resourceDMBean = getResourceDMBean(instance);
         if (resourceDMBean != null) {
            resourceDMBeans.add(resourceDMBean);
         }
      }
      return resourceDMBeans;
   }

   protected ResourceDMBean getResourceDMBean(Object instance) {
      ComponentMetadata md = instance != null ?
                             componentMetadataRepo.getComponentMetadata(instance.getClass()) : null;
      if (md == null || !md.isManageable())
         return null;

      ResourceDMBean resourceDMBean;
      try {
         resourceDMBean = new ResourceDMBean(instance, md.toManageableComponentMetadata());
      } catch (NoSuchFieldException | ClassNotFoundException e) {
         throw new CacheConfigurationException(e);
      }
      return resourceDMBean;
   }
}
