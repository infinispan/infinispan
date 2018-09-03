package org.infinispan.jmx;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.ComponentRef;
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
@SurvivesRestarts
@Scope(Scopes.GLOBAL)
public class CacheManagerJmxRegistration extends AbstractJmxRegistration {
   private static final Log log = LogFactory.getLog(CacheManagerJmxRegistration.class);
   public static final String CACHE_MANAGER_JMX_GROUP = "type=CacheManager";

   private boolean needToUnregister = false;
   private boolean stopped;
   private Collection<ResourceDMBean> resourceDMBeans;

   /**
    * On start, the mbeans are registered.
    */
   public void start() {
      initMBeanServer(globalConfig);

      if (mBeanServer != null) {
         Collection<ComponentRef<?>> components = basicComponentRegistry.getRegisteredComponents();
         resourceDMBeans = Collections.synchronizedCollection(getResourceDMBeansFromComponents(components));

         registrar.registerMBeans(resourceDMBeans);
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
            unregisterMBeans(resourceDMBeans);
            needToUnregister = false;
         } catch (Exception e) {
            log.problemsUnregisteringMBeans(e);
         }
      }
      stopped = true;
   }

   @Override
   protected ComponentsJmxRegistration buildRegistrar() {
      // Quote group name, to handle invalid ObjectName characters
      String groupName = CACHE_MANAGER_JMX_GROUP
            + "," + ComponentsJmxRegistration.NAME_KEY
            + "=" + ObjectName.quote(globalConfig.globalJmxStatistics().cacheManagerName());
      ComponentsJmxRegistration registrar = new ComponentsJmxRegistration(mBeanServer, groupName);
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

   public void unregisterCacheMBean(String cacheName, String cacheModeString) {
      if (mBeanServer != null) {
         String groupName = CacheJmxRegistration.CACHE_JMX_GROUP + "," + getCacheJmxName(cacheName, cacheModeString) +
                            ",manager=" + ObjectName.quote(globalConfig.globalJmxStatistics().cacheManagerName());
         String pattern = jmxDomain + ":" + groupName + ",*";
         try {
            Set<ObjectName> names = SecurityActions.queryNames(new ObjectName(pattern), null, mBeanServer);
            for (ObjectName name : names) {
               JmxUtil.unregisterMBean(name, mBeanServer);
            }
         } catch (MBeanRegistrationException e) {
            log.unableToUnregisterMBeanWithPattern(pattern, e);
         } catch (InstanceNotFoundException e) {
            // Ignore if Cache MBeans not present
         } catch (MalformedObjectNameException e) {
            String message = "Malformed pattern " + pattern;
            throw new CacheException(message, e);
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
   }

   String getCacheJmxName(String cacheName, String cacheModeString) {
      return ComponentsJmxRegistration.NAME_KEY + "=" + ObjectName.quote(
         cacheName + "(" + cacheModeString.toLowerCase() + ")");
   }

   public void registerMBean(Object managedComponent) {
      ResourceDMBean resourceDMBean = getResourceDMBean(managedComponent);
      registrar.registerMBeans(Collections.singleton(resourceDMBean));
      resourceDMBeans.add(resourceDMBean);
   }
}
