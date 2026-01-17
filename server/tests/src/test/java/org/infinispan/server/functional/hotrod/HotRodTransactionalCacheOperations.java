package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.infinispan.testing.Combinations;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.ParameterDeclarations;

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

   @InfinispanServer(ClusteredIT.class)
   public static TestClientDriver SERVERS;

   static final class ArgsProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ParameterDeclarations parameters, ExtensionContext context) {
         return Combinations.combine(Flag.class).stream()
               // Remove filter after https://github.com/infinispan/infinispan/issues/14926
               .filter(Set::isEmpty)
               .flatMap(f ->
                     Stream.of(TransactionMode.NON_XA, TransactionMode.NON_DURABLE_XA, TransactionMode.FULL_XA)
                           .map(mode -> Arguments.of(mode, f)));
      }
   }

   @ParameterizedTest(name = "{0}-{1}")
   @ArgumentsSource(ArgsProvider.class)
   public void testTransactionalCache(TransactionMode txMode, EnumSet<Flag> flags) throws Exception {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.remoteCache(SERVERS.getMethodName())
            .transactionMode(TransactionMode.NON_XA)
            .transactionManagerLookup(RemoteTransactionManagerLookup.getInstance());

      String xml = String.format(TEST_CACHE_XML_CONFIG, SERVERS.getMethodName(), txMode.name());

      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(config).withServerConfiguration(new StringConfiguration(xml))
            .<String, String>create().withFlags(flags.toArray(new Flag[0]));
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
