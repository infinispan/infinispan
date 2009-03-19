package org.horizon.jmx;

import org.horizon.config.GlobalConfiguration;
import org.horizon.factories.AbstractComponentRegistry;
import org.horizon.factories.GlobalComponentRegistry;
import org.horizon.factories.annotations.Inject;
import org.horizon.factories.annotations.NonVolatile;
import org.horizon.factories.annotations.Start;
import org.horizon.factories.annotations.Stop;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * // TODO: Mircea: Document this!
 *
 * @author
 */
@NonVolatile
public class PlatformMBeanServerGlobalRegistration {

   public static final String GLOBAL_JMX_GROUP = "[global]";
   private GlobalComponentRegistry registry;
   private GlobalConfiguration globalConfiguration;
   private MBeanServer mBeanServer;

   @Inject
   public void init(GlobalComponentRegistry registry, GlobalConfiguration configuration) {
      this.registry = registry;
      this.globalConfiguration = configuration;
   }

   @Start(priority = 20)
   public void start() {
      if (globalConfiguration.isExposeGlobalManagementStatistics()) {
         ComponentGroupJmxRegistration registrator = buildRegistrator();
         registrator.registerMBeans();
      }
   }

   public void setMBeanServer(MBeanServer mBeanServer) {
      this.mBeanServer = mBeanServer;
   }

   @Stop
   public void stop() {
      //this method might get called several times.
      // After the first call the cache will become null, so we guard this
      if (registry == null) return;
      if (globalConfiguration.isExposeGlobalManagementStatistics()) {
         ComponentGroupJmxRegistration componentGroupJmxRegistration = buildRegistrator();
         componentGroupJmxRegistration.unregisterCacheMBeans();
      }
      registry = null;
   }

   private ComponentGroupJmxRegistration buildRegistrator() {
      Set<AbstractComponentRegistry.Component> components = registry.getRegisteredComponents();
      MBeanServer platformMBeanServer;
      if (this.mBeanServer != null) {
         platformMBeanServer = this.mBeanServer;
      } else {
         platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
      }
      ComponentGroupJmxRegistration registrator = new ComponentGroupJmxRegistration(platformMBeanServer, components, GLOBAL_JMX_GROUP);
      if (globalConfiguration.getJmxDomain() != null) {
         registrator.setJmxDomain(globalConfiguration.getJmxDomain());
      }
      return registrator;
   }
}
