package org.infinispan.quarkus.server;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.functional.HotRodCacheOperations;
import org.infinispan.server.functional.HotRodCounterOperations;
import org.infinispan.server.functional.HotRodMultiMapOperations;
import org.infinispan.server.functional.HotRodTransactionalCacheOperations;
import org.infinispan.server.functional.RestOperations;
import org.infinispan.server.functional.RestRouter;
import org.infinispan.server.functional.RestServerResource;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * We must extend {@link ClusteredIT} so that we can specify the test classes required in the suite. All of these
 * tests rely on {@code InfinispanServerRule SERVERS = ClusteredIT.SERVERS;}, so it's not possible to simply execute
 * them outside of a suite as the containers are shutdown after the first test class has completed.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
      HotRodCacheOperations.class,
      HotRodCounterOperations.class,
      HotRodMultiMapOperations.class,
      HotRodTransactionalCacheOperations.class,
      RestOperations.class,
      RestRouter.class,
      RestServerResource.class
})
public class NativeClusteredIT extends ClusteredIT  {
}
