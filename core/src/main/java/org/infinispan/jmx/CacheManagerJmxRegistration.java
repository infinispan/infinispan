package org.infinispan.jmx;

import static org.infinispan.util.logging.Log.CONTAINER;

import javax.management.ObjectName;

import org.infinispan.commons.jmx.JmxUtil;
import org.infinispan.configuration.global.GlobalJmxStatisticsConfiguration;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Registers all the components from global component registry to the mbean server.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public final class CacheManagerJmxRegistration extends AbstractJmxRegistration {

   private static final String CACHE_MANAGER_JMX_GROUP = "type=CacheManager";

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
}
