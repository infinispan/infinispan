package org.infinispan.client.hotrod.query;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.util.Collections;
import java.util.Date;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.Indexer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

/**
 * Tests for multiple indexed caches in the server.
 */
@Test(testName = "client.hotrod.query.MultipleIndexedCacheTest", groups = "functional")
public class MultipleIndexedCacheTest extends MultiHotRodServersTest {

   private static final String USER_CACHE = "users";
   private static final String ACCOUNT_CACHE = "accounts";

   private static final int NODES = 3;
   private static final int NUM_ENTRIES = 50;

   private RemoteCache<Integer, UserPB> userCache;
   private RemoteCache<Integer, AccountPB> accountCache;

   public Configuration buildIndexedConfig() {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("sample_bank_account.User")
            .addIndexedEntity("sample_bank_account.Account");
      return builder.build();
   }

   public Configuration getNonIndexLockConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false).build();
   }

   public Configuration getNonIndexDataConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false).build();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = new ConfigurationBuilder();
      createHotRodServers(NODES, defaultConfiguration);

      cacheManagers.forEach(cm -> {
         cm.defineConfiguration(USER_CACHE, buildIndexedConfig());
         cm.defineConfiguration(ACCOUNT_CACHE, buildIndexedConfig());

         cm.getCache(USER_CACHE);
         cm.getCache(ACCOUNT_CACHE);
      });

      waitForClusterToForm(USER_CACHE, ACCOUNT_CACHE);

      userCache = client(0).getCache(USER_CACHE);
      accountCache = client(0).getCache(ACCOUNT_CACHE);
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   @Test
   public void testMassIndexing() {
      for (int i = 0; i < NUM_ENTRIES; i++) {
         AccountPB account = new AccountPB();
         account.setId(i);
         account.setDescription("account" + i);
         account.setCreationDate(new Date());
         accountCache.put(account.getId(), account);

         UserPB user = new UserPB();
         user.setId(i);
         user.setName("name" + i);
         user.setSurname("surname" + i);
         user.setAccountIds(Collections.singleton(i));
         userCache.put(user.getId(), user);
      }

      assertEquals(query("sample_bank_account.Account", accountCache, "description", "'account1'"), 1);
      assertEquals(query("sample_bank_account.User", userCache, "name", "'name1'"), 1);

      reindex(ACCOUNT_CACHE);

      assertEquals(query("sample_bank_account.Account", accountCache, "description", "'account1'"), 1);
      assertEquals(query("sample_bank_account.User", userCache, "name", "'name1'"), 1);

      reindex(USER_CACHE);

      assertEquals(query("sample_bank_account.Account", accountCache, "description", "'account1'"), 1);
      assertEquals(query("sample_bank_account.User", userCache, "name", "'name1'"), 1);
   }

   private void reindex(String cacheName) {
      Cache<?, ?> cache = cacheManagers.get(0).getCache(cacheName);
      Indexer indexer = org.infinispan.query.Search.getIndexer(cache);
      CompletionStages.join(indexer.run());
   }

   private <T> long query(String entity, RemoteCache<?, ?> cache, String fieldName, String fieldValue) {
      QueryFactory qf = Search.getQueryFactory(cache);
      Query<T> q = qf.create("FROM " + entity + " WHERE " + fieldName + " = " + fieldValue);
      return q.execute().hitCount().orElse(-1);
   }
}
