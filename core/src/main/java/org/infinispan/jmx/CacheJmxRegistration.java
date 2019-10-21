package org.infinispan.jmx;

import javax.management.ObjectName;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

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
public final class CacheJmxRegistration extends AbstractJmxRegistration {

   private static final String CACHE_JMX_GROUP = "type=Cache";

   @Inject
   Configuration cacheConfiguration;

   @Inject
   CacheManagerJmxRegistration globalJmxRegistration;

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject
   String cacheName;

   @Start(priority = 14)
   @Override
   public void start() {
      synchronized (globalJmxRegistration) {
         if (mBeanServer == null) {
            mBeanServer = globalJmxRegistration.mBeanServer;
         }
         if (jmxDomain == null) {
            jmxDomain = globalJmxRegistration.jmxDomain;
         }
      }

      super.start();
   }

   @Stop
   @Override
   public void stop() {
      super.stop();
   }

   @Override
   protected String initGroup() {
      return CACHE_JMX_GROUP + "," + NAME_KEY + "="
            + ObjectName.quote(cacheName + "(" + cacheConfiguration.clustering().cacheModeString().toLowerCase() + ")")
            + ",manager=" + ObjectName.quote(globalConfig.cacheManagerName());
   }

   @Override
   protected String initDomain() {
      return globalJmxRegistration.jmxDomain;
   }
}
