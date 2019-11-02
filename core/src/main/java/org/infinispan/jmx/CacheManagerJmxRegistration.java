package org.infinispan.jmx;

import javax.management.ObjectName;

import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.DefaultCacheManager;

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

   private static final String GROUP_PATTERN = TYPE + "=CacheManager," + NAME + "=%s";

   public CacheManagerJmxRegistration() {
      super(DefaultCacheManager.OBJECT_NAME);
   }

   @Override
   protected String initGroup() {
      return String.format(GROUP_PATTERN, ObjectName.quote(globalConfig.cacheManagerName()));
   }
}
