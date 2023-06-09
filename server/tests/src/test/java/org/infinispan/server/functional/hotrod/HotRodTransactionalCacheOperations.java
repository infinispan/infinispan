package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import jakarta.transaction.TransactionManager;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @author Pedro Ruivo
 * @since 10.0
 **/
public class HotRodTransactionalCacheOperations {
   private static final String TEST_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <distributed-cache-configuration name=\"%s\">" +
               "    <locking isolation=\"REPEATABLE_READ\"/>" +
               "    <transaction locking=\"PESSIMISTIC\" mode=\"%s\" />" +
               "  </distributed-cache-configuration>" +
               "</cache-container></infinispan>";

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @ParameterizedTest(name = "{0}")
   @EnumSource(value = Parser.TransactionMode.class, names = {"NON_XA", "NON_DURABLE_XA", "FULL_XA"})
   public void testTransactionalCache(Parser.TransactionMode txMode) throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.remoteCache(SERVERS.getMethodName())
            .transactionMode(TransactionMode.NON_XA)
            .transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());

      String xml = String.format(TEST_CACHE_XML_CONFIG, SERVERS.getMethodName(), txMode.name());

      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(config).withServerConfiguration(new StringConfiguration(xml)).create();
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
}
