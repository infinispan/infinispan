package org.infinispan.server.test.client.hotrod;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.server.test.category.HotRodClustered;
import org.infinispan.server.test.category.HotRodSingleNode;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for remote iteration in compat mode with primitive values and default (JBoss) marshalling.
 */
@RunWith(Arquillian.class)
@Category({HotRodSingleNode.class, HotRodClustered.class})
public class HotRodRemoteCacheCompatIT {

   private static final String CACHE_NAME = "compatibilityCache";
   private static final int CACHE_SIZE = 1000;
   private static RemoteCacheManager remoteCacheManager;

   private RemoteCache<Object, Object> remoteCache;

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   @Before
   public void setup() {
      RemoteCacheManagerFactory remoteCacheManagerFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
              .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
              .port(server1.getHotrodEndpoint().getPort());
      remoteCacheManager = remoteCacheManagerFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);
   }

   @AfterClass
   public static void release() {
      if (remoteCacheManager != null) {
         remoteCacheManager.getCache(CACHE_NAME).clear();
         remoteCacheManager.stop();
      }
   }

   @Test
   public void testIterationWithPrimitiveValues() {
      remoteCache.clear();
      IntStream.range(0, CACHE_SIZE).forEach(k -> remoteCache.put(k, "value" + k));
      Set<Object> keys = new HashSet<>();
      try (CloseableIterator<Entry<Object, Object>> iter = remoteCache.retrieveEntries(null, 10)) {
         iter.forEachRemaining(e -> keys.add(e.getKey()));
      }
      assertEquals(CACHE_SIZE, keys.size());
   }
}
