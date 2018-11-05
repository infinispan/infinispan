package org.infinispan.cli.interpreter;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.jmx.ComponentsJmxRegistration;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.util.logging.LogFactory;
import org.kohsuke.MetaInfServices;

@MetaInfServices(org.infinispan.lifecycle.ModuleLifecycle.class)
public class LifecycleCallbacks implements ModuleLifecycle {
   private static final Log log = LogFactory.getLog(LifecycleCallbacks.class, Log.class);

   private ObjectName interpreterObjName;

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      // This works because the interpreter is not yet used internally, otherwise it would have to be in cacheManagerStarting
      GlobalJmxStatisticsConfiguration globalCfg = gcr.getGlobalConfiguration().globalJmxStatistics();
      if (globalCfg.enabled()) {
         MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(globalCfg.mbeanServerLookup(), globalCfg.properties());
         String groupName = getGroupName(globalCfg.cacheManagerName());
         Interpreter interpreter = new Interpreter();

         gcr.registerComponent(interpreter, Interpreter.class);

         // Pick up metadata from the component metadata repository
         ManageableComponentMetadata meta = gcr.getComponentMetadataRepo().findComponentMetadata(Interpreter.class)
               .toManageableComponentMetadata();
         // And use this metadata when registering the transport as a dynamic MBean
         try {
            ResourceDMBean mbean = new ResourceDMBean(interpreter, meta);
            interpreterObjName = new ObjectName(String.format("%s:%s,component=Interpreter", globalCfg.domain(), groupName));
            JmxUtil.registerMBean(mbean, interpreterObjName, mbeanServer);
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
