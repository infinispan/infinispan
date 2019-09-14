package org.infinispan.jmx;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Registers all the components from global component registry to the mbean server.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class CacheManagerJmxRegistration extends AbstractJmxRegistration {

   private static final Log log = LogFactory.getLog(CacheManagerJmxRegistration.class);

   private static final String CACHE_MANAGER_JMX_GROUP = "type=CacheManager";

   private boolean needToUnregister = false;
   private boolean stopped = false;
   private Collection<ResourceDMBean> resourceDMBeans;

   /**
    * On start, the mbeans are registered.
    */
   @Override
   public void start() {
      super.start();

      if (mBeanServer != null) {
         resourceDMBeans = Collections.synchronizedCollection(getResourceDMBeansFromComponents());

         internalRegister(resourceDMBeans);
         needToUnregister = true;
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
            internalUnregister(resourceDMBeans);
            needToUnregister = false;
         } catch (Exception e) {
            log.problemsUnregisteringMBeans(e);
         }
      }
      stopped = true;
   }

   @Override
   protected String initGroup() {
      return CACHE_MANAGER_JMX_GROUP + "," + NAME_KEY + "=" + ObjectName.quote(globalConfig.cacheManagerName());
   }

   @Override
   protected String initDomain() {
      GlobalJmxStatisticsConfiguration globalJmxConfig = globalConfig.globalJmxStatistics();
      String jmxDomain = JmxUtil.buildJmxDomain(globalJmxConfig.domain(), mBeanServer, getGroupName());
      if (!globalJmxConfig.allowDuplicateDomains() && !jmxDomain.equals(globalJmxConfig.domain())) {
         throw CONTAINER.jmxMBeanAlreadyRegistered(getGroupName(), globalJmxConfig.domain());
      }
      return jmxDomain;
   }

   public void unregisterCacheMBean(String cacheName, String cacheModeString) {
      // Unregisters cache and everything from same group
      if (mBeanServer != null) {
         String nameFilter = getDomain() + ":" + CacheJmxRegistration.getCacheGroupName(cacheName, cacheModeString, globalConfig.cacheManagerName()) + ",*";
         try {
            Set<ObjectName> names = SecurityActions.queryNames(new ObjectName(nameFilter), null, mBeanServer);
            for (ObjectName name : names) {
               try {
                  JmxUtil.unregisterMBean(name, mBeanServer);
               } catch (MBeanRegistrationException e) {
                  log.unableToUnregisterMBean(name.toString(), e);
               } catch (InstanceNotFoundException e) {
                  // Ignore if MBean not present
               }
            }
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
   }

   @Override
   protected void trackRegisteredResourceDMBean(ResourceDMBean resourceDMBean) {
      resourceDMBeans.add(resourceDMBean);
   }
}
