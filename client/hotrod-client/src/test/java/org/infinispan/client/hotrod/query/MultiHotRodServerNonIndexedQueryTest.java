package org.infinispan.client.hotrod.query;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * Tests query without indexing over Hot Rod in a two-node cluster.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
@Test(testName = "client.hotrod.query.MultiHotRodServerNonIndexedQueryTest", groups = "functional")
public class MultiHotRodServerNonIndexedQueryTest extends MultiHotRodServerQueryTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      createHotRodServers(2, builder);

      waitForClusterToForm();

      remoteCache0 = client(0).getCache();
      remoteCache1 = client(1).getCache();
   }
}
