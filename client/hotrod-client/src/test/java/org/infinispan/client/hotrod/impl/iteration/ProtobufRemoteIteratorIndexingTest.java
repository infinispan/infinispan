package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.testng.annotations.Test;

/**
 * @since 9.1
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.ProtobufRemoteIteratorIndexingTest")
public class ProtobufRemoteIteratorIndexingTest extends MultiHotRodServersTest implements AbstractRemoteIteratorTest {

   private static final int NUM_NODES = 2;
   private static final int CACHE_SIZE = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("sample_bank_account.Account");
      createHotRodServers(NUM_NODES, hotRodCacheConfiguration(cfg));
      waitForClusterToForm();
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   public void testSimpleIteration() {
      RemoteCache<Integer, AccountPB> cache = clients.get(0).getCache();

      populateCache(CACHE_SIZE, this::newAccountPB, cache);

      List<AccountPB> results = new ArrayList<>();
      cache.retrieveEntries(null, null, CACHE_SIZE).forEachRemaining(e -> results.add((AccountPB) e.getValue()));

      assertEquals(CACHE_SIZE, results.size());
   }

   public void testFilteredIterationWithQuery() {
      RemoteCache<Integer, AccountPB> remoteCache = clients.get(0).getCache();
      populateCache(CACHE_SIZE, this::newAccountPB, remoteCache);
      QueryFactory queryFactory = Search.getQueryFactory(remoteCache);

      int lowerId = 5;
      int higherId = 8;
      Query<Account> simpleQuery = queryFactory.<Account>create("FROM sample_bank_account.Account WHERE id BETWEEN :lowerId AND :higherId")
                                      .setParameter("lowerId", lowerId)
                                      .setParameter("higherId", higherId);
      Set<Entry<Object, Object>> entries = extractEntries(remoteCache.retrieveEntriesByQuery(simpleQuery, null, 10));
      Set<Integer> keys = extractKeys(entries);

      assertEquals(4, keys.size());
      assertForAll(keys, key -> key >= lowerId && key <= higherId);
      assertForAll(entries, e -> e.getValue() instanceof AccountPB);

      Query<Object[]> projectionsQuery = queryFactory.<Object[]>create("SELECT id, description FROM sample_bank_account.Account WHERE id BETWEEN :lowerId AND :higherId")
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
