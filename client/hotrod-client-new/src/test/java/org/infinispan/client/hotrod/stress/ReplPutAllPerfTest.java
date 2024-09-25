package org.infinispan.client.hotrod.stress;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "stress", testName="org.infinispan.client.hotrod.stress.ReplPutAllPerfTest", timeOut = 15*60*1000)
public class ReplPutAllPerfTest extends AbstractPutAllPerfTest {

   @Override
   protected int numberOfHotRodServers() {
      return 5;
   }

   @Override
   protected ConfigurationBuilder clusterConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(
            CacheMode.REPL_SYNC, false));
   }
}
