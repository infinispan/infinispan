package org.infinispan.client.hotrod;

import org.infinispan.config.Configuration;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional" , testName = "client.hotrod.DistTopologyChangeTest")
public class DistTopologyChange extends ReplTopologyChangeTest {
   protected Configuration.CacheMode getCacheMode() {
      return Configuration.CacheMode.DIST_SYNC;
   }
}
