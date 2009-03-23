package org.horizon.jmx;

import org.horizon.config.GlobalConfiguration;
import org.horizon.factories.AbstractComponentRegistry;
import org.horizon.factories.GlobalComponentRegistry;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.annotations.Start;
import org.horizon.factories.annotations.Stop;

import javax.management.MBeanServer;
import java.util.Set;

/**
 * Registers all the components from global component registry to the mbean server.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@NonVolatile
public class CacheManagerJmxRegistration {

   public static final String GLOBAL_JMX_GROUP = "[global]";
   private GlobalComponentRegistry registry;
   private GlobalConfiguration globalConfiguration;
   private MBeanServer mBeanServer;

   @Inject
   public void init(GlobalComponentRegistry registry, GlobalConfiguration configuration) {
      this.registry = registry;
      this.globalConfiguration = configuration;
   }

   /**
    * On start, the mbeans are registered.
    */
   @Start(priority = 20)
   public void start() {
      if (globalConfiguration.isExposeGlobalJmxStatistics()) {
         ComponentsJmxRegistration registrator = buildRegistrator();
         registrator.registerMBeans();
      }
   }

   public void setMBeanServer(MBeanServer mBeanServer) {
      this.mBeanServer = mBeanServer;
   }

   /**
    * On stop, the mbeans are unregistered.
    */
   @Stop
   public void stop() {
      //this method might get called several times.
      // After the first call the cache will become null, so we guard this
      if (registry == null) return;
      if (globalConfiguration.isExposeGlobalJmxStatistics()) {
         ComponentsJmxRegistration componentsJmxRegistration = buildRegistrator();
         componentsJmxRegistration.unregisterMBeans();
      }
      registry = null;
   }

   private ComponentsJmxRegistration buildRegistrator() {
      Set<AbstractComponentRegistry.Component> components = registry.getRegisteredComponents();
      mBeanServer = CacheJmxRegistration.getMBeanServer(globalConfiguration);
      ComponentsJmxRegistration registrator = new ComponentsJmxRegistration(mBeanServer, components, GLOBAL_JMX_GROUP);
      CacheJmxRegistration.updateDomain(registrator, registry, mBeanServer);
      return registrator;
   }
}
