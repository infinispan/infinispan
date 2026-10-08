package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.client.hotrod.impl.iteration.Util.assertForAll;
import static org.infinispan.client.hotrod.impl.iteration.Util.extractEntries;
import static org.infinispan.client.hotrod.impl.iteration.Util.extractKeys;
import static org.infinispan.client.hotrod.impl.iteration.Util.populateCache;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.query.Query;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.protostream.sampledomain.bank.Account;
import org.testng.annotations.Test;

/**
 * @since 9.1
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.ProtobufRemoteIteratorIndexingTest")
public class ProtobufRemoteIteratorIndexingTest extends MultiHotRodServersTest {

   private static final int NUM_NODES = 2;
   private static final int CACHE_SIZE = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Account.ENTITY_NAME);
      createHotRodServers(NUM_NODES, hotRodCacheConfiguration(cfg));
      waitForClusterToForm();
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   public void testSimpleIteration() {
      RemoteCache<Integer, Account> cache = clients.get(0).getCache();

      populateCache(CACHE_SIZE, Util::newAccount, cache);

      List<Account> results = new ArrayList<>();
      cache.retrieveEntries(null, null, CACHE_SIZE).forEachRemaining(e -> results.add((Account) e.getValue()));

      assertEquals(CACHE_SIZE, results.size());
   }

   public void testFilteredIterationWithQuery() {
      RemoteCache<Integer, Account> remoteCache = clients.get(0).getCache();
      populateCache(CACHE_SIZE, Util::newAccount, remoteCache);

      int lowerId = 5;
      int higherId = 8;
      Query<Account> simpleQuery = remoteCache.query("FROM sample_domain.Account WHERE id BETWEEN :lowerId AND :higherId");
      simpleQuery
            .setParameter("lowerId", lowerId)
            .setParameter("higherId", higherId);
      Set<Entry<Object, Object>> entries = extractEntries(remoteCache.retrieveEntriesByQuery(simpleQuery, null, 10));
      Set<Integer> keys = extractKeys(entries);

      assertEquals(4, keys.size());
      assertForAll(keys, key -> key >= lowerId && key <= higherId);
      assertForAll(entries, e -> e.getValue() instanceof Account);

      Query<Object[]> projectionsQuery = remoteCache.query("SELECT id, description FROM sample_domain.Account WHERE id BETWEEN :lowerId AND :higherId");
      projectionsQuery
            .setParameter("lowerId", lowerId)
            .setParameter("higherId", higherId);
      Set<Entry<Integer, Object[]>> entriesWithProjection = extractEntries(remoteCache.retrieveEntriesByQuery(projectionsQuery, null, 10));

      assertEquals(4, entriesWithProjection.size());
      assertForAll(entriesWithProjection, entry -> {
         Integer id = entry.getKey();
         Object[] projection = entry.getValue();
         return projection[0].equals(id) && projection[1].equals("description for " + id);
      });
   }
}
