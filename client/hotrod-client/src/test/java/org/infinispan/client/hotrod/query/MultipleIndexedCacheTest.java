package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for multiple indexed caches in the server.
 */
@Test(testName = "client.hotrod.query.MultipleIndexedCacheTest", groups = "functional")
public class MultipleIndexedCacheTest extends MultiHotRodServersTest {

   private static final String USER_CACHE = "users";
   private static final String ACCOUNT_CACHE = "accounts";

   private static final String DATA_CACHE = "lucene_data_dist";
   private static final String METADATA_CACHE = "lucene_metadata_dist";
   private static final String LOCKING_CACHE = "lucene_locking_dist";

   private static final int NODES = 3;
   private static final int NUM_ENTRIES = 50;

   private RemoteCache<Integer, UserPB> userCache;
   private RemoteCache<Integer, AccountPB> accountCache;

   public Configuration buildIndexedConfig(String lockCache, String dataCache, String metadataCache) {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().index(Index.PRIMARY_OWNER)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
            .addProperty("default.metadata_cachename", metadataCache)
            .addProperty("default.data_cachename", dataCache)
            .addProperty("default.locking_cachename", lockCache);
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
         cm.defineConfiguration(USER_CACHE, buildIndexedConfig(LOCKING_CACHE, DATA_CACHE, METADATA_CACHE));
         cm.defineConfiguration(ACCOUNT_CACHE, buildIndexedConfig(LOCKING_CACHE, DATA_CACHE, METADATA_CACHE));

         cm.defineConfiguration(METADATA_CACHE, getNonIndexDataConfig());
         cm.defineConfiguration(DATA_CACHE, getNonIndexDataConfig());
         cm.defineConfiguration(LOCKING_CACHE, getNonIndexLockConfig());

         cm.getCache(USER_CACHE);
         cm.getCache(ACCOUNT_CACHE);
      });

      waitForClusterToForm(USER_CACHE, ACCOUNT_CACHE);

      userCache = client(0).getCache(USER_CACHE);
      accountCache = client(0).getCache(ACCOUNT_CACHE);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort).marshaller(new ProtoStreamMarshaller());
   }

   @BeforeClass(alwaysRun = true)
   protected void registerSerCtx() throws Exception {
      ProtobufMetadataManager protobufMetadataManager = manager(0).getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      protobufMetadataManager.registerProtofile("bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      for (RemoteCacheManager rcm : clients) {
         MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(rcm));
      }
   }

   @Test
   public void testMassIndexing() {
      getAccountsPB().forEach(a -> accountCache.put(a.getId(), a));
      getUsersPB().forEach(u -> userCache.put(u.getId(), u));

      assertEquals(query(AccountPB.class, accountCache, "description:account1"), 1);
      assertEquals(query(UserPB.class, userCache, "name:name1"), 1);

      reindex(ACCOUNT_CACHE);

      assertEquals(query(AccountPB.class, accountCache, "description:account1"), 1);
      assertEquals(query(UserPB.class, userCache, "name:name1"), 1);

      reindex(USER_CACHE);

      assertEquals(query(AccountPB.class, accountCache, "description:account1"), 1);
      assertEquals(query(UserPB.class, userCache, "name:name1"), 1);

   }

   private void reindex(String cacheName) {
      Cache<?, ?> cache = cacheManagers.get(0).getCache(cacheName);
      MassIndexer massIndexer = org.infinispan.query.Search.getSearchManager(cache).getMassIndexer();
      massIndexer.start();
   }

   private <T> int query(Class<T> entity, RemoteCache<?, ?> cache, String query) {
      String[] fields = query.split(":");
      Query q = Search.getQueryFactory(cache).from(entity).having(fields[0]).eq(fields[1]).build();
      return q.list().size();
   }

   private List<AccountPB> getAccountsPB() {
      return IntStream.range(0, NUM_ENTRIES).boxed().map(i -> {
         AccountPB accountPB = new AccountPB();
         accountPB.setId(i);
         accountPB.setDescription("account" + i);
         accountPB.setCreationDate(new Date());
         return accountPB;
      }).collect(Collectors.toList());
   }

   private List<UserPB> getUsersPB() {
      return IntStream.range(0, NUM_ENTRIES).boxed().map(i -> {
         UserPB userPB = new UserPB();
         userPB.setId(i);
         userPB.setName("name" + i);
         userPB.setSurname("surname" + i);
         return userPB;
      }).collect(Collectors.toList());
   }

}
