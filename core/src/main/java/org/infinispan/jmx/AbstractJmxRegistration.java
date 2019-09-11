package org.infinispan.jmx;

import java.util.ArrayList;
import java.util.Collection;

import javax.management.MBeanServer;

import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Parent class for top level JMX component registration.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.NONE)
public abstract class AbstractJmxRegistration {
   private static final Log log = LogFactory.getLog(AbstractJmxRegistration.class);

   protected ComponentsJmxRegistration registrar;
   @Inject GlobalConfiguration globalConfig;
   @Inject BasicComponentRegistry basicComponentRegistry;

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
         if (instance != null) {
            ResourceDMBean resourceDMBean = getResourceDMBean(instance, component.getName());
            if (resourceDMBean != null) {
               resourceDMBeans.add(resourceDMBean);
            }
         }
      }
      return resourceDMBeans;
   }

   protected ResourceDMBean getResourceDMBean(Object instance, String componentName) {
      MBeanMetadata md = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
      if (md == null)
         return null;
      return new ResourceDMBean(instance, md, componentName);
   }
}
