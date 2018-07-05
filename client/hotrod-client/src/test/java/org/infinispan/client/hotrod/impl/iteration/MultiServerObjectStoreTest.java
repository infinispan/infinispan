package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.iteration.MultiServerObjectStoreTest")
public class MultiServerObjectStoreTest extends MultiHotRodServersTest implements AbstractRemoteIteratorTest {

   private static final int NUM_SERVERS = 2;
   private static final int CACHE_SIZE = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private org.infinispan.configuration.cache.ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.clustering().hash().numSegments(60).numOwners(1);
      builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      return builder;
   }

   @Test
   public void testIteration() throws Exception {
      RemoteCache<Integer, String> remoteCache = clients.get(0).getCache();
      populateCache(CACHE_SIZE, i -> "value", remoteCache);
      Set<Map.Entry<Object, Object>> entries = extractEntries(remoteCache.retrieveEntries(null, 5));

      assertEquals(CACHE_SIZE, entries.size());
   }
}
