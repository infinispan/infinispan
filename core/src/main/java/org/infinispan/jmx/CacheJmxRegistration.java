package org.infinispan.jmx;

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

/**
 * If {@link org.infinispan.configuration.cache.Configuration#statistics()} is enabled, then class will register all
 * the MBeans from cache local's ConfigurationRegistry to the MBean server.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public final class CacheJmxRegistration extends AbstractJmxRegistration {

   private static final String GROUP_PATTERN = TYPE + "=Cache," + NAME + "=%s," + MANAGER + "=%s";

   @Inject
   Configuration cacheConfiguration;

   @Inject
   CacheManagerJmxRegistration globalJmxRegistration;

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject
   String cacheName;

   public CacheJmxRegistration() {
      super(CacheImpl.OBJECT_NAME);
   }

   @Start(priority = 14)
   @Override
   public void start() {
      // prevent double lookup of MBeanServer on eventual restart
      if (mBeanServer == null && globalJmxRegistration.mBeanServer != null) {
         groupName = initGroup();
         // grab domain and MBean server from container
         mBeanServer = globalJmxRegistration.mBeanServer;
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
      return String.format(GROUP_PATTERN,
            ObjectName.quote(cacheName + "(" + cacheConfiguration.clustering().cacheModeString().toLowerCase() + ")"),
            ObjectName.quote(globalConfig.cacheManagerName()));
   }
}
