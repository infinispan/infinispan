package org.infinispan.cli.interpreter;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.jmx.ComponentsJmxRegistration;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.logging.LogFactory;

@InfinispanModule(name = "cli-interpreter", requiredModules = "core")
public class LifecycleCallbacks implements ModuleLifecycle {
   private static final Log log = LogFactory.getLog(LifecycleCallbacks.class, Log.class);

   private ObjectName interpreterObjName;

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      GlobalJmxStatisticsConfiguration jmxConfiguration = gcr.getGlobalConfiguration().globalJmxStatistics();
      if (jmxConfiguration.enabled()) {
         String groupName = getGroupName(globalConfiguration.cacheManagerName());
         Interpreter interpreter = new Interpreter();

         gcr.registerComponent(interpreter, Interpreter.class);

         try {
            CacheManagerJmxRegistration jmxRegistration = gcr.getComponent(CacheManagerJmxRegistration.class);
            jmxRegistration.registerExternalMBean(interpreter, jmxConfiguration.domain(), groupName, "Interpreter");
         } catch (Exception e) {
            interpreterObjName = null;
            log.jmxRegistrationFailed();
         }
      }
   }

   private String getGroupName(String name) {
      return CacheManagerJmxRegistration.CACHE_MANAGER_JMX_GROUP + "," + ComponentsJmxRegistration.NAME_KEY + "="
            + ObjectName.quote(name);
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      if (interpreterObjName != null) {
         GlobalJmxStatisticsConfiguration jmxConfig = gcr.getGlobalConfiguration().globalJmxStatistics();
         MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(jmxConfig.mbeanServerLookup(), jmxConfig.properties());
         try {
            JmxUtil.unregisterMBean(interpreterObjName, mbeanServer);
         } catch (Exception e) {
            log.jmxUnregistrationFailed();
         }
      }
   }
}
