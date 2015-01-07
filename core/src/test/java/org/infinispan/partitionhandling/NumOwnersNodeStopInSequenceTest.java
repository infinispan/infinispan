package org.infinispan.partitionhandling;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * With a cluster made out of nodes {A,B,C,D}, tests that D stops gracefully and before the state transfer finishes,
 * another node C also stops. {A,B} should enter degraded mode.
 * The only way in which it could recover is explicitly, through JMX operations.
 */
@Test(groups = "functional", testName = "partitionhandling.NumOwnersNodeStopInSequenceTest")
public class NumOwnersNodeStopInSequenceTest extends NumOwnersNodeCrashInSequenceTest {

   @Override
   protected void crashCacheManagers(EmbeddedCacheManager... cacheManagers) {
      TestingUtil.killCacheManagers(cacheManagers);
   }
}
