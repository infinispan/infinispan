package org.infinispan.server.test.client.hotrod;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import javax.transaction.TransactionManager;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.server.test.category.HotRodClustered;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Simple Transactional Test to verify if the cache is decorated correctly.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@RunWith(Arquillian.class)
@Category(HotRodClustered.class)
public class HotRodTransactionalCacheIT {

   private static final String TEST_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <distributed-cache-configuration name=\"test\">" +
               "    <locking isolation=\"REPEATABLE_READ\"/>" +
               "    <transaction locking=\"PESSIMISTIC\" mode=\"NON_XA\" />" +
               "  </distributed-cache-configuration>" +
               "</cache-container></infinispan>";

   @InfinispanResource("container1")
   RemoteInfinispanServer server1;

   private RemoteCacheManager remoteCacheManager;

   @Before
   public void initialize() {
      if (remoteCacheManager == null) {
         Configuration config = createRemoteCacheManagerConfiguration();
         remoteCacheManager = new RemoteCacheManager(config, true);
      }
   }

   @Test
   public void testCommitAndRollback() throws Exception {
      remoteCacheManager.administration().createCache("test", new XMLStringConfiguration(TEST_CACHE_XML_CONFIG));
      RemoteCache<String, String> cache = remoteCacheManager.getCache("test");
      TransactionManager tm = cache.getTransactionManager();
      tm.begin();
      cache.put("k", "v1");
      assertEquals("v1", cache.get("k"));
      tm.commit();

      assertEquals("v1", cache.get("k"));

      tm.begin();
      cache.put("k", "v2");
      cache.put("k2", "v1");
      assertEquals("v2", cache.get("k"));
      assertEquals("v1", cache.get("k2"));
      tm.rollback();

      assertEquals("v1", cache.get("k"));
      assertNull(cache.get("k2"));
   }

   private Configuration createRemoteCacheManagerConfiguration() {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.addServer()
            .host(server1.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server1.getHotrodEndpoint().getPort());

      config.transaction().transactionMode(TransactionMode.NON_XA);
      config.transaction().transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());
      return config.build();
   }

}
