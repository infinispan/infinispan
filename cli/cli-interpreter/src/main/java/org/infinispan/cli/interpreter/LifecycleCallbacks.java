package org.infinispan.cli.interpreter;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.jmx.CacheManagerJmxRegistration;
import org.infinispan.jmx.ComponentsJmxRegistration;
import org.infinispan.jmx.JmxUtil;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.lifecycle.AbstractModuleLifecycle;
import org.infinispan.util.logging.LogFactory;

public class LifecycleCallbacks extends AbstractModuleLifecycle {
   private static final Log log = LogFactory.getLog(LifecycleCallbacks.class, Log.class);

   private ObjectName interpreterObjName;

   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
      MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);
      String groupName = getGroupName(globalCfg);
      String jmxDomain = globalCfg.globalJmxStatistics().domain();
      Interpreter interpreter = new Interpreter();

      gcr.registerComponent(interpreter, Interpreter.class);

      // Pick up metadata from the component metadata repository
      ManageableComponentMetadata meta = gcr.getComponentMetadataRepo().findComponentMetadata(Interpreter.class)
            .toManageableComponentMetadata();
      // And use this metadata when registering the transport as a dynamic MBean
      try {
         ResourceDMBean mbean = new ResourceDMBean(interpreter, meta);
         interpreterObjName = new ObjectName(String.format("%s:%s,component=Interpreter", jmxDomain, groupName));
         JmxUtil.registerMBean(mbean, interpreterObjName, mbeanServer);
      } catch (Exception e) {
         interpreterObjName = null;
         log.jmxRegistrationFailed();
      }
   }

   private String getGroupName(GlobalConfiguration globalCfg) {
      return CacheManagerJmxRegistration.CACHE_MANAGER_JMX_GROUP + "," + ComponentsJmxRegistration.NAME_KEY + "="
            + ObjectName.quote(globalCfg.globalJmxStatistics().cacheManagerName());
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      if (interpreterObjName != null) {
         GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
         MBeanServer mbeanServer = JmxUtil.lookupMBeanServer(globalCfg);
         try {
            JmxUtil.unregisterMBean(interpreterObjName, mbeanServer);
         } catch (Exception e) {
            log.jmxUnregistrationFailed();
         }
      }
   }
}
