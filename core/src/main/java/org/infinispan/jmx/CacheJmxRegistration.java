package org.infinispan.jmx;

import java.util.ArrayList;
import java.util.Collection;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * If {@link org.infinispan.configuration.cache.Configuration#jmxStatistics()} is enabled, then class will register all
 * the MBeans from cache local's ConfigurationRegistry to the MBean server.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @see java.lang.management.ManagementFactory#getPlatformMBeanServer()
 * @since 4.0
 */
@SurvivesRestarts
public class CacheJmxRegistration extends AbstractJmxRegistration {
   private static final Log log = LogFactory.getLog(CacheJmxRegistration.class);
   public static final String CACHE_JMX_GROUP = "type=Cache";

   @Inject private Configuration cacheConfiguration;
   @Inject public CacheManagerJmxRegistration globalJmxRegistration;
   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject private String cacheName;

   private Collection<ResourceDMBean> nonCacheDMBeans;

   private boolean needToUnregister = false;

   private volatile boolean unregisterCacheMBean;

   /**
    * Here is where the registration is being performed.
    */
   @Start(priority = 14)
   public void start() {
      initMBeanServer(globalConfig);

      if (mBeanServer != null) {
         Collection<ComponentRef<?>> components = basicComponentRegistry.getRegisteredComponents();
         Collection<ResourceDMBean> resourceDMBeans = getResourceDMBeansFromComponents(components);
         nonCacheDMBeans = getNonCacheComponents(resourceDMBeans);

         registrar.registerMBeans(resourceDMBeans);
         needToUnregister = true;
         log.mbeansSuccessfullyRegistered();
      }
   }

   /**
    * Unregister when the cache is being stopped.
    */
   @Stop
   public void stop() {
      if (needToUnregister) {
         // Only unregister the non cache MBean so that it can be restarted
         try {
            unregisterMBeans(nonCacheDMBeans);
            needToUnregister = false;
         } catch (Exception e) {
            log.problemsUnregisteringMBeans(e);
         }
      }

      // If removing cache, also remove cache MBean
      if (unregisterCacheMBean)
         globalJmxRegistration.unregisterCacheMBean(this.cacheName, this.cacheConfiguration
            .clustering().cacheModeString());

      // make sure we don't set cache to null, in case it needs to be restarted via JMX.
   }

   public void setUnregisterCacheMBean(boolean unregisterCacheMBean) {
      this.unregisterCacheMBean = unregisterCacheMBean;
   }

   @Override
   protected ComponentsJmxRegistration buildRegistrar() {
      // Quote group name, to handle invalid ObjectName characters
      String groupName = CACHE_JMX_GROUP + "," + globalJmxRegistration.getCacheJmxName(cacheName,
                                                                                       cacheConfiguration.clustering().cacheModeString()) + ",manager=" + ObjectName.quote(globalConfig.globalJmxStatistics().cacheManagerName());
      ComponentsJmxRegistration registrar = new ComponentsJmxRegistration(mBeanServer, groupName);
      updateDomain(registrar, mBeanServer, groupName, globalJmxRegistration);
      return registrar;
   }

   protected void updateDomain(ComponentsJmxRegistration registrar,
                               MBeanServer mBeanServer, String groupName,
                               CacheManagerJmxRegistration globalJmxRegistration) {
      if (!globalConfig.globalJmxStatistics().enabled() && jmxDomain == null) {
         String tmpJmxDomain = JmxUtil.buildJmxDomain(globalConfig.globalJmxStatistics().domain(), mBeanServer, groupName);
         synchronized (globalJmxRegistration) {
            if (globalJmxRegistration.jmxDomain == null) {
               if (!tmpJmxDomain.equals(globalConfig.globalJmxStatistics().domain()) && !globalConfig.globalJmxStatistics().allowDuplicateDomains()) {
                  throw log.jmxMBeanAlreadyRegistered(tmpJmxDomain, globalConfig.globalJmxStatistics().domain());
               }
               // Set manager component's jmx domain so that other caches under same manager
               // can see it, particularly important when jmx is only enabled at the cache level
               globalJmxRegistration.jmxDomain = tmpJmxDomain;
            }
            // So that all caches share the same domain, regardless of whether dups are
            // allowed or not, simply assign the manager's calculated jmxDomain
            jmxDomain = globalJmxRegistration.jmxDomain;
         }
      } else {
         // If global stats were enabled, manager's jmxDomain would have been populated
         // when cache manager was started, so no need for synchronization here.
         jmxDomain = globalJmxRegistration.jmxDomain == null ? globalConfig.globalJmxStatistics().domain() : globalJmxRegistration.jmxDomain;
      }
      registrar.setJmxDomain(jmxDomain);
   }

   protected Collection<ResourceDMBean> getNonCacheComponents(Collection<ResourceDMBean> components) {
      Collection<ResourceDMBean> componentsExceptCache = new ArrayList<>(64);
      for (ResourceDMBean component : components) {
         if (!CacheImpl.OBJECT_NAME.equals(component.getObjectName())) {
            componentsExceptCache.add(component);
         }
      }
      return componentsExceptCache;
   }

}
