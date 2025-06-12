package org.infinispan.server.functional;

import org.infinispan.server.functional.extensions.PojoMarshalling;
import org.infinispan.server.functional.hotrod.HotRodAdmin;
import org.infinispan.server.functional.hotrod.HotRodCacheContinuousQueries;
import org.infinispan.server.functional.hotrod.HotRodCacheOperations;
import org.infinispan.server.functional.hotrod.HotRodClientMetrics;
import org.infinispan.server.functional.hotrod.HotRodListenerWithDslFilter;
import org.infinispan.server.functional.hotrod.HotRodMultiMapOperations;
import org.infinispan.server.test.core.rollingupgrade.RollingUpgradeConfigurationBuilder;
import org.infinispan.server.test.junit5.InfinispanSuite;
import org.infinispan.server.test.junit5.RollingUpgradeHandlerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * @author William Burns &lt;will@infinispan.org&gt;
 * @since 16.0
 **/
@Suite(failIfNoTests = false)
@SelectClasses({
      HotRodCacheOperations.class,
//      RestOperations.class,
//      RestRouter.class,
//      RestServerResource.class,
//      MemcachedOperations.class,
      HotRodAdmin.class,
//      HotRodCounterOperations.class,
      HotRodMultiMapOperations.class,
//      HotRodTransactionalCacheOperations.class,
//      HotRodCacheEvents.class,
//      HotRodCacheQueries.class,
      HotRodCacheContinuousQueries.class,
      HotRodListenerWithDslFilter.class,
//      IgnoreCaches.class,
//      RestLoggingResource.class,
//      ScriptingTasks.class,
//      ServerTasks.class,
      PojoMarshalling.class,
      HotRodClientMetrics.class
})
public class ClusteredRollingUpgradeIT extends InfinispanSuite {

   static {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder("15.2.0.Final", "15.2.1.Final");
      SERVERS = new RollingUpgradeHandlerExtension(builder);
   }

   @RegisterExtension
   public static final RollingUpgradeHandlerExtension SERVERS;
}
