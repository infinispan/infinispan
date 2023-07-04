package org.infinispan.server.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.SyncStrongCounter;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.TestSystemPropertyNames;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import jakarta.transaction.TransactionManager;

/**
 * @since 15.0
 **/
public class HotRodCompCacheOperationsIT {

   private static final String TX_CACHE_CONFIG =
         "<distributed-cache name=\"%s\">\n"
               + "    <encoding media-type=\"application/x-protostream\"/>\n"
               + "    <transaction mode=\"NON_XA\"/>\n"
               + "</distributed-cache>";
   private static final String MULTIMAP_CACHE_CONFIG =
         "<distributed-cache name=\"%s\">\n"
               + "    <encoding media-type=\"application/x-protostream\"/>\n"
               + "</distributed-cache>";

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/ClusteredServerIspn13Test.xml")
               .numServers(2)
               .property(TestSystemPropertyNames.INFINISPAN_TEST_SERVER_BASE_IMAGE_NAME, "quay.io/infinispan/server:13.0")
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Test
   public void testHotRodOperations() {
      RemoteCache<String, String> cache = SERVERS.hotrod().create();
      cache.put("k1", "v1");
      assertEquals(1, cache.size());
      assertEquals("v1", cache.get("k1"));
      cache.remove("k1");
      assertEquals(0, cache.size());
   }

   @Test
   public void testCounter() {
      String counterName = SERVERS.getMethodName();
      CounterManager counterManager = SERVERS.getCounterManager();
      assertNull(counterManager.getConfiguration(counterName));
      assertTrue(counterManager.defineCounter(counterName, CounterConfiguration.builder(CounterType.BOUNDED_STRONG).build()));
      SyncStrongCounter strongCounter = counterManager.getStrongCounter(counterName).sync();
      assertEquals(0, strongCounter.getValue());
      assertEquals(10, strongCounter.addAndGet(10));
   }

   @Test
   public void testTransaction() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.remoteCache(SERVERS.getMethodName())
            .transactionMode(TransactionMode.NON_XA)
            .transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());

      String xml = String.format(TX_CACHE_CONFIG, SERVERS.getMethodName());

      RemoteCache<String, String> cache = SERVERS.hotrod()
            .withClientConfiguration(config)
            .withServerConfiguration(new StringConfiguration(xml))
            .create();
      TransactionManager tm = cache.getTransactionManager();
      tm.begin();
      cache.put("k", "v");
      assertEquals("v", cache.get("k"));
      tm.commit();
      assertEquals("v", cache.get("k"));
   }

   @Test
   public void testMultiMap() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.remoteCache(SERVERS.getMethodName());

      String xml = String.format(MULTIMAP_CACHE_CONFIG, SERVERS.getMethodName());

      RemoteCacheManager rcm = SERVERS.hotrod()
            .withClientConfiguration(config)
            .withServerConfiguration(new StringConfiguration(xml))
            .create().getRemoteCacheManager();

      MultimapCacheManager<String, String> multimapCacheManager = RemoteMultimapCacheManagerFactory.from(rcm);

      RemoteMultimapCache<String, String> people = multimapCacheManager.get(SERVERS.getMethodName());

      people.put("coders", "Will");
      people.put("coders", "Auri");
      people.put("coders", "Pedro");

      Collection<String> coders = people.get("coders").join();
      assertTrue(coders.contains("Will"));
      assertTrue(coders.contains("Auri"));
      assertTrue(coders.contains("Pedro"));
   }

   @Test
   public void testMultiMapWithDuplicates() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.remoteCache(SERVERS.getMethodName());

      String xml = String.format(MULTIMAP_CACHE_CONFIG, SERVERS.getMethodName());

      RemoteCacheManager rcm = SERVERS.hotrod()
            .withClientConfiguration(config)
            .withServerConfiguration(new StringConfiguration(xml))
            .create().getRemoteCacheManager();

      MultimapCacheManager<String, String> multimapCacheManager = RemoteMultimapCacheManagerFactory.from(rcm);

      RemoteMultimapCache<String, String> people = multimapCacheManager.get(SERVERS.getMethodName(), true);

      people.put("coders", "Will");
      people.put("coders", "Will");
      people.put("coders", "Auri");
      people.put("coders", "Pedro");

      Collection<String> coders = people.get("coders").join();
      assertTrue(coders.contains("Will"));
      assertTrue(coders.contains("Auri"));
      assertTrue(coders.contains("Pedro"));
   }
}
