package org.infinispan.client.hotrod.graalvm;

import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.functional.hotrod.HotRodAdmin;
import org.infinispan.server.functional.hotrod.HotRodCacheOperations;
import org.infinispan.server.functional.hotrod.HotRodCacheQueries;
import org.infinispan.server.functional.hotrod.HotRodCounterOperations;
import org.infinispan.server.functional.hotrod.HotRodMultiMapOperations;
import org.infinispan.server.functional.hotrod.HotRodTransactionalCacheOperations;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
      HotRodCacheOperations.class,
      HotRodAdmin.class,
      HotRodCounterOperations.class,
      HotRodMultiMapOperations.class,
      HotRodTransactionalCacheOperations.class,
      HotRodCacheQueries.class
})
public class GraalClusteredTest extends ClusteredIT {
}
