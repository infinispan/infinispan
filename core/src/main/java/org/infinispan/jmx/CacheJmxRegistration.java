package org.infinispan.jmx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import javax.management.ObjectName;

import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * If {@link org.infinispan.configuration.cache.Configuration#jmxStatistics()} is enabled, then class will register all
 * the MBeans from cache local's ConfigurationRegistry to the MBean server.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public class CacheJmxRegistration extends AbstractJmxRegistration {

   private static final Log log = LogFactory.getLog(CacheJmxRegistration.class);

   private static final String CACHE_JMX_GROUP = "type=Cache";

   @Inject
   Configuration cacheConfiguration;

   @Inject
   CacheManagerJmxRegistration globalJmxRegistration;

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject
   String cacheName;

   /**
    * Tracks all managed cache components, except the cache itself.
    */
   private Collection<ResourceDMBean> nonCacheDMBeans;
   private boolean componentsRegistered = false;
   private boolean cacheRegistered = false;
   private volatile boolean unregisterCacheMBean = false;

   /**
    * Here is where the registration is being performed.
    */
   @Start(priority = 14)
   @Override
   public void start() {
      if (mBeanServer == null) {
         mBeanServer = globalJmxRegistration.mBeanServer;
      }
      if (jmxDomain == null) {
         jmxDomain = globalJmxRegistration.jmxDomain;
      }

      super.start();

      if (mBeanServer != null) {
         Collection<ResourceDMBean> resourceDMBeans = getResourceDMBeansFromComponents();
         nonCacheDMBeans = Collections.synchronizedCollection(getNonCacheComponents(resourceDMBeans));

         internalRegister(cacheRegistered ? nonCacheDMBeans : resourceDMBeans);
         cacheRegistered = true;
         componentsRegistered = true;
         log.mbeansSuccessfullyRegistered();
      }
   }

   /**
    * Unregister when the cache is being stopped.
    */
   @Stop
   public void stop() {
      if (componentsRegistered) {
         // Only unregister the non cache MBeans so that the cache can be restarted via JMX
         try {
            internalUnregister(nonCacheDMBeans);
            componentsRegistered = false;
         } catch (Exception e) {
            log.problemsUnregisteringMBeans(e);
         }
      }

      // If removing cache, also remove cache MBean
      if (unregisterCacheMBean) {
         globalJmxRegistration.unregisterCacheMBean(cacheName, cacheConfiguration.clustering().cacheModeString());
         unregisterCacheMBean = false;
         cacheRegistered = false;
      }
   }

   /**
    * Indicates that the cache MBean is to be unregistered on stop. This is not normally done because we keep he MBean
    * around to allow restart via jmx).
    *
    * @param unregisterCacheMBean
    */
   public void setUnregisterCacheMBean(boolean unregisterCacheMBean) {
      this.unregisterCacheMBean = unregisterCacheMBean;
   }

   @Override
   protected String initGroup() {
      return getCacheGroupName(cacheName, cacheConfiguration.clustering().cacheModeString(), globalConfig.cacheManagerName());
   }

   @Override
   protected String initDomain() {
      return globalJmxRegistration.jmxDomain;
   }

   static String getCacheGroupName(String cacheName, String cacheModeString, String cacheManagerName) {
      return CACHE_JMX_GROUP + "," + NAME_KEY + "="
            + ObjectName.quote(cacheName + "(" + cacheModeString.toLowerCase() + ")")
            + ",manager=" + ObjectName.quote(cacheManagerName);
   }

   private Collection<ResourceDMBean> getNonCacheComponents(Collection<ResourceDMBean> components) {
      Collection<ResourceDMBean> componentsExceptCache = new ArrayList<>(64);
      for (ResourceDMBean component : components) {
         if (!CacheImpl.OBJECT_NAME.equals(component.getMBeanName())) {
            componentsExceptCache.add(component);
         }
      }
      return componentsExceptCache;
   }

   @Override
   protected void trackRegisteredResourceDMBean(ResourceDMBean resourceDMBean) {
      nonCacheDMBeans.add(resourceDMBean);
   }
}
