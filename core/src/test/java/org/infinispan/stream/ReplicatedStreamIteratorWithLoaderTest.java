package org.infinispan.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Test to verify replicated stream behavior when a loader is present
 *
 * @author afield
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.ReplicatedStreamIteratorWithLoaderTest")
public class ReplicatedStreamIteratorWithLoaderTest extends BaseStreamIteratorWithLoaderTest {

   public ReplicatedStreamIteratorWithLoaderTest() {
      super(false, CacheMode.REPL_SYNC, "ReplicatedStreamIteratorWithLoaderTest");
   }
}
