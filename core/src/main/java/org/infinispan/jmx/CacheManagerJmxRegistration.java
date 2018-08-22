package org.infinispan.jmx;

import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.factories.AbstractComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Registers all the components from global component registry to the mbean server.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@SurvivesRestarts
public class CacheManagerJmxRegistration extends AbstractJmxRegistration {
   private static final Log log = LogFactory.getLog(CacheManagerJmxRegistration.class);
   public static final String CACHE_MANAGER_JMX_GROUP = "type=CacheManager";

   @Inject private GlobalComponentRegistry globalReg;
   private boolean needToUnregister = false;
   private boolean stopped;

   /**
    * On start, the mbeans are registered.
    */
   public void start() {
      if (registerMBeans(globalReg.getRegisteredComponents(), globalConfig)) {
         needToUnregister = true;
      } else {
         log.unableToRegisterCacheManagerMBeans();
      }
      stopped = false;
   }

   /**
    * On stop, the mbeans are unregistered.
    */
   public void stop() {
      // This method might get called several times.
      if (stopped) return;
      if (needToUnregister) {
         try {
            unregisterMBeans(globalReg.getRegisteredComponents());
            needToUnregister = false;
         } catch (Exception e) {
            log.problemsUnregisteringMBeans(e);
         }
      }
      stopped = true;
   }

   @Override
   protected ComponentsJmxRegistration buildRegistrar(Set<AbstractComponentRegistry.Component> components) {
      // Quote group name, to handle invalid ObjectName characters
      String groupName = CACHE_MANAGER_JMX_GROUP
            + "," + ComponentsJmxRegistration.NAME_KEY
            + "=" + ObjectName.quote(globalConfig.globalJmxStatistics().cacheManagerName());
      ComponentsJmxRegistration registrar = new ComponentsJmxRegistration(mBeanServer, components, groupName);
      updateDomain(registrar, mBeanServer, groupName);
      return registrar;
   }

   protected void updateDomain(ComponentsJmxRegistration registrar, MBeanServer mBeanServer, String groupName) {
      if (jmxDomain == null) {
         jmxDomain = JmxUtil.buildJmxDomain(globalConfig.globalJmxStatistics().domain(), mBeanServer, groupName);
         String configJmxDomain = globalConfig.globalJmxStatistics().domain();
         if (!jmxDomain.equals(configJmxDomain) && !globalConfig.globalJmxStatistics().allowDuplicateDomains()) {
            throw log.jmxMBeanAlreadyRegistered(groupName, configJmxDomain);
         }
      }
      registrar.setJmxDomain(jmxDomain);
   }

}
