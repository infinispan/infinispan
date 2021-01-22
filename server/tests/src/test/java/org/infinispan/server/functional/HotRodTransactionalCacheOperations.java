package org.infinispan.server.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collection;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.configuration.parsing.Parser;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @author Pedro Ruivo
 * @since 10.0
 **/
@RunWith(Parameterized.class)
public class HotRodTransactionalCacheOperations {
   private static final String TEST_CACHE_XML_CONFIG =
         "<infinispan><cache-container>" +
               "  <distributed-cache-configuration name=\"%s\">" +
               "    <locking isolation=\"REPEATABLE_READ\"/>" +
               "    <transaction locking=\"PESSIMISTIC\" mode=\"%s\" />" +
               "  </distributed-cache-configuration>" +
               "</cache-container></infinispan>";

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Parameterized.Parameters(name = "{0}")
   public static Collection<Object[]> data() {
      ArrayList<Object[]> txModes = new ArrayList<>();
      txModes.add(new Object[]{Parser.TransactionMode.NON_XA});
      txModes.add(new Object[]{Parser.TransactionMode.NON_DURABLE_XA});
      txModes.add(new Object[]{Parser.TransactionMode.FULL_XA});
      return txModes;
   }

   private final Parser.TransactionMode txMode;

   public HotRodTransactionalCacheOperations(Parser.TransactionMode txMode) {
      this.txMode = txMode;
   }

   @Test
   public void testTransactionalCache() throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.remoteCache(SERVER_TEST.getMethodName())
            .transactionMode(TransactionMode.NON_XA)
            .transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());

      String xml = String.format(TEST_CACHE_XML_CONFIG, SERVER_TEST.getMethodName(), txMode.name());

      RemoteCache<String, String> cache = SERVER_TEST.hotrod().withClientConfiguration(config).withServerConfiguration(new XMLStringConfiguration(xml)).create();
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
