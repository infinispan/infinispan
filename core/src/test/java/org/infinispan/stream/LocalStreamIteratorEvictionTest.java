package org.infinispan.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Eviction test for stream when using a LOCAL cache to verify it works properly
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.LocalStreamIteratorEvictionTest")
public class LocalStreamIteratorEvictionTest extends BaseStreamIteratorEvictionTest {
   public LocalStreamIteratorEvictionTest() {
      super(false, CacheMode.LOCAL);
   }
}
