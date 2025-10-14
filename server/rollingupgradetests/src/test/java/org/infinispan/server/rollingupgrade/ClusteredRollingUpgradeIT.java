package org.infinispan.server.rollingupgrade;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.functional.extensions.PojoMarshalling;
import org.infinispan.server.functional.extensions.ScriptingTasks;
import org.infinispan.server.functional.extensions.ServerTasks;
import org.infinispan.server.functional.hotrod.HotRodAdmin;
import org.infinispan.server.functional.hotrod.HotRodCacheContinuousQueries;
import org.infinispan.server.functional.hotrod.HotRodCacheEvents;
import org.infinispan.server.functional.hotrod.HotRodCacheOperations;
import org.infinispan.server.functional.hotrod.HotRodCacheQueries;
import org.infinispan.server.functional.hotrod.HotRodClientMetrics;
import org.infinispan.server.functional.hotrod.HotRodCounterOperations;
import org.infinispan.server.functional.hotrod.HotRodFlagCacheOperations;
import org.infinispan.server.functional.hotrod.HotRodListenerWithDslFilter;
import org.infinispan.server.functional.hotrod.HotRodMultiMapOperations;
import org.infinispan.server.functional.hotrod.HotRodTransactionalCacheOperations;
import org.infinispan.server.functional.hotrod.IgnoreCaches;
import org.infinispan.server.functional.memcached.MemcachedOperations;
import org.infinispan.server.functional.resp.RespDistributedTest;
import org.infinispan.server.functional.resp.RespHashTest;
import org.infinispan.server.functional.resp.RespListTest;
import org.infinispan.server.functional.resp.RespPubSubTest;
import org.infinispan.server.functional.resp.RespScriptTest;
import org.infinispan.server.functional.resp.RespSetTest;
import org.infinispan.server.functional.resp.RespSortedSetTest;
import org.infinispan.server.functional.resp.RespStringTest;
import org.infinispan.server.functional.resp.RespTransactionTest;
import org.infinispan.server.functional.rest.RestContainerListenerTest;
import org.infinispan.server.functional.rest.RestLoggingResource;
import org.infinispan.server.functional.rest.RestOperations;
import org.infinispan.server.functional.rest.RestProtobufResourceTest;
import org.infinispan.server.functional.rest.RestRouter;
import org.infinispan.server.functional.rest.RestServerResource;
import org.infinispan.server.test.artifacts.Artifacts;
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
      HotRodFlagCacheOperations.class,
      RestOperations.class,
      RestRouter.class,
      RestServerResource.class,
      MemcachedOperations.class,
      HotRodAdmin.class,
      HotRodCounterOperations.class,
      HotRodMultiMapOperations.class,
      HotRodTransactionalCacheOperations.class,
      HotRodCacheEvents.class,
      HotRodCacheQueries.class,
      HotRodCacheContinuousQueries.class,
      HotRodListenerWithDslFilter.class,
      IgnoreCaches.class,
      RestLoggingResource.class,
      ScriptingTasks.class,
      ServerTasks.class,
      PojoMarshalling.class,
      HotRodClientMetrics.class,
      RespDistributedTest.class,
      RespHashTest.class,
      RespListTest.class,
      RespPubSubTest.class,
      RespScriptTest.class,
      RespSetTest.class,
      RespSortedSetTest.class,
      RespStringTest.class,
      RespTransactionTest.class,
      RestContainerListenerTest.class,
      RestProtobufResourceTest.class,
})
public class ClusteredRollingUpgradeIT extends InfinispanSuite {

   static {
      RollingUpgradeConfigurationBuilder builder = new RollingUpgradeConfigurationBuilder(ClusteredRollingUpgradeIT.class.getName(),
            RollingUpgradeTestUtil.getFromVersion(), RollingUpgradeTestUtil.getToVersion())
            .nodeCount(ClusteredIT.SERVER_COUNT)
            .useCustomServerConfiguration("configuration/ClusteredServerTest.xml")
            .addMavenArtifacts(ClusteredIT.mavenArtifacts())
            .addArchives(Artifacts.artifacts());
      SERVERS = new RollingUpgradeHandlerExtension(builder);
   }

   @RegisterExtension
   public static final RollingUpgradeHandlerExtension SERVERS;
}
