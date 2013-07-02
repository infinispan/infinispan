package org.infinispan.client.hotrod;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional" , testName = "client.hotrod.DistTopologyChangeTest")
public class DistTopologyChangeTest extends ReplTopologyChangeTest {
   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
