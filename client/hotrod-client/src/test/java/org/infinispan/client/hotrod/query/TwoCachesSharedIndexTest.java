package org.infinispan.client.hotrod.query;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;
import java.util.List;

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
import org.infinispan.query.dsl.Query;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests remote query using two caches and a shared index
 *
 * @author gustavonalle
 * @since 9.0
 */
@Test(testName = "client.hotrod.query.TwoCachesSharedIndexTest", groups = "functional")
public class TwoCachesSharedIndexTest extends MultiHotRodServersTest {

   private static final String USER_CACHE = "users";
   private static final String ACCOUNT_CACHE = "accounts";

   public static final String USER_METADATA = "user_metadata";
   public static final String USER_DATA = "user_data";
   public static final String USER_LOCKING = "user_locking";

   public static final String ACCOUNT_METADATA = "account_metadata";
   public static final String ACCOUNT_DATA = "account_data";
   public static final String ACCOUNT_LOCKING = "account_locking";

   public Configuration buildIndexedConfig(String lockCache, String dataCache, String metadataCache) {
      ConfigurationBuilder builder = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().index(Index.LOCAL)
              .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
              .addProperty("default.metadata_cachename", metadataCache)
              .addProperty("default.data_cachename", dataCache)
              .addProperty("default.locking_cachename", lockCache)
              .addProperty("lucene_version", "LUCENE_CURRENT");
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
      createHotRodServers(2, defaultConfiguration);

      cacheManagers.forEach(cm -> {
         cm.defineConfiguration(USER_CACHE, buildIndexedConfig(USER_LOCKING, USER_DATA, USER_METADATA));
         cm.defineConfiguration(ACCOUNT_CACHE, buildIndexedConfig(ACCOUNT_LOCKING, ACCOUNT_DATA, ACCOUNT_METADATA));

         cm.defineConfiguration(ACCOUNT_METADATA, getNonIndexDataConfig());
         cm.defineConfiguration(USER_METADATA, getNonIndexDataConfig());
         cm.defineConfiguration(ACCOUNT_DATA, getNonIndexDataConfig());
         cm.defineConfiguration(USER_DATA, getNonIndexDataConfig());
         cm.defineConfiguration(USER_LOCKING, getNonIndexLockConfig());
         cm.defineConfiguration(ACCOUNT_LOCKING, getNonIndexLockConfig());

         cm.getCache(USER_CACHE);
         cm.getCache(ACCOUNT_CACHE);
      });

      waitForClusterToForm(USER_CACHE, ACCOUNT_CACHE);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(String host, int serverPort) {
      return super.createHotRodClientConfigurationBuilder(host, serverPort).marshaller(new ProtoStreamMarshaller());
   }

   @BeforeClass(alwaysRun = true)
   protected void registerSerCtx() throws Exception {
      ProtobufMetadataManager protobufMetadataManager = manager(0).getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      protobufMetadataManager.registerProtofile("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      for (RemoteCacheManager rcm : clients) {
         MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(rcm));
      }
   }

   @Test
   public void testWithUserCache() throws IOException {
      RemoteCache<Integer, UserPB> userCache = client(0).getCache(USER_CACHE);
      userCache.put(1, getUserPB());

      Query query = Search.getQueryFactory(userCache).from(UserPB.class).having("name").eq("John").build();
      List<UserPB> users = query.list();

      assertEquals("John", users.iterator().next().getName());
   }

   @Test
   public void testWithAccountCache() throws IOException {
      RemoteCache<Integer, AccountPB> accountCache = client(0).getCache(ACCOUNT_CACHE);
      accountCache.put(1, getAccountPB());

      Query query = Search.getQueryFactory(accountCache).from(AccountPB.class).having("description").eq("account1").build();
      List<AccountPB> accounts = query.list();

      assertEquals(accounts.iterator().next().getDescription(), "account1");
   }

   private AccountPB getAccountPB() {
      AccountPB accountPB = new AccountPB();
      accountPB.setId(1);
      accountPB.setDescription("account1");
      accountPB.setCreationDate(new Date());
      return accountPB;
   }

   private UserPB getUserPB() {
      UserPB userPB = new UserPB();
      userPB.setName("John");
      userPB.setSurname("Doe");
      return userPB;
   }

}
