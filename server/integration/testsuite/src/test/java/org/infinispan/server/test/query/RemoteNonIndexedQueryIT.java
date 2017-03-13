package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.category.SingleNode;
import org.infinispan.server.test.util.ManagementClient;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for remote queries over HotRod on a local non-indexed cache.
 *
 * @author Adrian Nistor
 * @author Martin Gencur
 * @since 7.0
 */
@Category({SingleNode.class})
@RunWith(Arquillian.class)
public class RemoteNonIndexedQueryIT extends RemoteQueryIT {

   private static final String CACHE_TEMPLATE = "localNotIndexedCacheConfiguration";
   private static final String CACHE_CONTAINER = "local";
   private static final String TEST_CACHE = "localNotIndexed";

   @InfinispanResource("container1")
   protected RemoteInfinispanServer server;

   public RemoteNonIndexedQueryIT() {
      super(CACHE_CONTAINER, TEST_CACHE);
   }

   @BeforeClass
   public static void beforeClass() throws Exception {
      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.addCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
      client.addCache(TEST_CACHE, CACHE_CONTAINER, CACHE_TEMPLATE, ManagementClient.CacheType.LOCAL);
   }

   @AfterClass
   public static void afterClass() throws Exception {
      ManagementClient client = ManagementClient.getStandaloneInstance();
      client.removeCache(TEST_CACHE, CACHE_CONTAINER, ManagementClient.CacheType.LOCAL);
      client.removeCacheConfiguration(CACHE_TEMPLATE, CACHE_CONTAINER, ManagementClient.CacheTemplate.LOCAL);
   }

   @Override
   protected RemoteInfinispanServer getServer() {
      return server;
   }
}
