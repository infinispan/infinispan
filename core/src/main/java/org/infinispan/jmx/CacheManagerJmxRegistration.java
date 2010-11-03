package org.infinispan.jmx;

import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.AbstractComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.Set;

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
   private GlobalComponentRegistry globalReg;

   @Inject
   public void init(GlobalComponentRegistry registry, GlobalConfiguration configuration) {
      this.globalReg = registry;
      this.globalConfig = configuration;
   }

   /**
    * On start, the mbeans are registered.
    */
   public void start() {
      if (globalConfig.isExposeGlobalJmxStatistics()) {
         registerMBeans(globalReg.getRegisteredComponents(), globalConfig);
      }
   }

   /**
    * On stop, the mbeans are unregistered.
    */
   public void stop() {
      // This method might get called several times.
      // After the first call the cache will become null, so we guard this
      if (globalReg == null) return;
      if (globalConfig.isExposeGlobalJmxStatistics()) {
         unregisterMBeans(globalReg.getRegisteredComponents());
      }
      globalReg = null;
   }

   @Override
   protected ComponentsJmxRegistration buildRegistrar(Set<AbstractComponentRegistry.Component> components) {
      // Quote group name, to handle invalid ObjectName characters      
      String groupName = CACHE_MANAGER_JMX_GROUP
            + "," + ComponentsJmxRegistration.NAME_KEY
            + "=" + ObjectName.quote(globalConfig.getCacheManagerName());
      ComponentsJmxRegistration registrar = new ComponentsJmxRegistration(mBeanServer, components, groupName);
      updateDomain(registrar, mBeanServer, groupName);
      return registrar;
   }

   protected void updateDomain(ComponentsJmxRegistration registrar, MBeanServer mBeanServer, String groupName) {
      if (jmxDomain == null) {
         jmxDomain = getJmxDomain(globalConfig.getJmxDomain(), mBeanServer, groupName);
         String configJmxDomain = globalConfig.getJmxDomain();
         if (!jmxDomain.equals(configJmxDomain) && !globalConfig.isAllowDuplicateDomains()) {
            String message = "There's already an cache manager instance registered under '" + configJmxDomain +
                  "' JMX domain. If you want to allow multiple instances configured with same JMX domain enable " +
                  "'allowDuplicateDomains' attribute in 'globalJmxStatistics' config element";
            if (log.isErrorEnabled()) log.error(message);
            throw new JmxDomainConflictException(message);
         }
      }
      registrar.setJmxDomain(jmxDomain);
   }

}
