package org.infinispan.cli.interpreter;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commons.jmx.JmxUtil;
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
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      // This works because the interpreter is not yet used internally, otherwise it would have to be in cacheManagerStarting
      GlobalJmxStatisticsConfiguration globalCfg = gcr.getGlobalConfiguration().globalJmxStatistics();
      if (globalCfg.enabled()) {
         String groupName = getGroupName(globalCfg.cacheManagerName());
         Interpreter interpreter = new Interpreter();

         gcr.registerComponent(interpreter, Interpreter.class);

         try {
            CacheManagerJmxRegistration jmxRegistration = gcr.getComponent(CacheManagerJmxRegistration.class);
            jmxRegistration.registerExternalMBean(interpreter, globalCfg.domain(), groupName, "Interpreter");
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
