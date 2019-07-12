package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodMultiMapOperations {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testMultiMap() {
      RemoteCache<String, String> cache = SERVER_TEST.getHotRodCache(CacheMode.DIST_SYNC);
      MultimapCacheManager multimapCacheManager = RemoteMultimapCacheManagerFactory.from(cache.getRemoteCacheManager());

      RemoteMultimapCache<Integer, String> people = multimapCacheManager.get(cache.getName());
      people.put(1, "Elaia");
      people.put(1, "Oihana");

      Collection<String> littles = people.get(1).join();

      assertEquals(2, littles.size());
      assertTrue(littles.contains("Elaia"));
      assertTrue(littles.contains("Oihana"));
   }
}
