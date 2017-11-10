package org.infinispan.jmx;

import java.util.Set;

import javax.management.MBeanServer;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.AbstractComponentRegistry;
import org.infinispan.factories.annotations.Inject;

/**
 * Parent class for top level JMX component registration.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractJmxRegistration {
   String jmxDomain;
   MBeanServer mBeanServer;
   @Inject GlobalConfiguration globalConfig;

   protected abstract ComponentsJmxRegistration buildRegistrar(Set<AbstractComponentRegistry.Component> components);

   /**
    * Registers a set of MBean components and returns true if successfully registered; false otherwise.
    * @param components components to register
    * @param globalConfig global configuration
    * @return true if successfully registered; false otherwise.
    */
   protected boolean registerMBeans(Set<AbstractComponentRegistry.Component> components, GlobalConfiguration globalConfig) {
      try {
         mBeanServer = JmxUtil.lookupMBeanServer(globalConfig);
      } catch (Exception e) {
         mBeanServer = null;
      }

      if (mBeanServer != null) {
         ComponentsJmxRegistration registrar = buildRegistrar(components);
         registrar.registerMBeans();
         return true;
      } else {
         return false;
      }
   }

   protected void unregisterMBeans(Set<AbstractComponentRegistry.Component> components) {
      if (mBeanServer != null) {
         ComponentsJmxRegistration registrar = buildRegistrar(components);
         registrar.unregisterMBeans();
      }
   }

}
