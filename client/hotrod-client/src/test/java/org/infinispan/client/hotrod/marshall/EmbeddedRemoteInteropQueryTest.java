package org.infinispan.client.hotrod.marshall;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.CurrencyMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.GenderMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.NotIndexedMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.AbstractSerializationContextInitializer;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.TransactionHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests interoperability between remote query and embedded mode.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.marshall.EmbeddedRemoteInteropQueryTest", groups = "functional")
@CleanupAfterMethod
public class EmbeddedRemoteInteropQueryTest extends SingleCacheManagerTest {

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, Account> remoteCache;
   private Cache<?, ?> embeddedCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = createConfigBuilder();
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalBuilder.serialization().addContextInitializers(new ServerSCI());

      cacheManager = TestCacheManagerFactory.createServerModeCacheManager(globalBuilder, builder);
      cache = cacheManager.getCache();

      embeddedCache = cache.getAdvancedCache().withEncoding(IdentityEncoder.class);

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort())
            .addContextInitializers(TestDomainSCI.INSTANCE, NotIndexedSCI.INSTANCE);

      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
      return cacheManager;
   }

   protected org.infinispan.configuration.cache.ConfigurationBuilder createConfigBuilder() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.indexing().enable()
             .addIndexedEntities(UserHS.class, AccountHS.class, TransactionHS.class)
             .addProperty("default.directory_provider", "local-heap")
             .addProperty("lucene_version", "LUCENE_CURRENT");
      return builder;
   }

   @Override
   protected void teardown() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
      super.teardown();
   }

   public void testPutAndGet() {
      Account account = createAccountPB(1);
      remoteCache.put(1, account);

      // try to get the object through the local cache interface and check it's the same object we put
      assertEquals(1, embeddedCache.keySet().size());
      Object key = embeddedCache.keySet().iterator().next();
      Object localObject = embeddedCache.get(key);
      assertAccount((Account) localObject, AccountHS.class);

      // get the object through the remote cache interface and check it's the same object we put
      Account fromRemoteCache = remoteCache.get(1);
      assertAccount(fromRemoteCache, AccountPB.class);
   }

   public void testPutAndGetForEmbeddedEntry() {
      AccountHS account = new AccountHS();
      account.setId(1);
      account.setDescription("test description");
      account.setCreationDate(new Date(42));
      cache.put(1, account);

      // try to get the object through the remote cache interface and check it's the same object we put
      assertEquals(1, remoteCache.keySet().size());
      Map.Entry<Integer, Account> entry = remoteCache.entrySet().iterator().next();
      assertAccount(entry.getValue(), AccountPB.class);

      // get the object through the embedded cache interface and check it's the same object we put
      Account fromEmbeddedCache = (Account) embeddedCache.get(1);
      assertAccount(fromEmbeddedCache, AccountHS.class);
   }

   public void testRemoteQuery() {
      Account account = createAccountPB(1);
      remoteCache.put(1, account);

      // get account back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Account> query = qf.create("FROM sample_bank_account.Account WHERE description LIKE '%test%'");
      List<Account> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertAccount(list.get(0), AccountPB.class);
   }

   public void testRemoteQueryForEmbeddedEntry() {
      AccountHS account = new AccountHS();
      account.setId(1);
      account.setDescription("test description");
      account.setCreationDate(new Date(42));
      cache.put(1, account);

      // get account back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Account> query = qf.create("FROM sample_bank_account.Account WHERE description LIKE '%test%'");
      List<Account> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertAccount(list.get(0), AccountPB.class);
   }

   public void testRemoteQueryForEmbeddedEntryOnNonIndexedField() {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setGender(User.Gender.MALE);
      user.setNotes("1234567890");
      cache.put(1, user);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User WHERE notes LIKE '%567%'");
      List<User> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals(UserPB.class, list.get(0).getClass());
      assertEquals(1, list.get(0).getId());
      assertEquals("1234567890", list.get(0).getNotes());
   }

   public void testRemoteQueryForEmbeddedEntryOnNonIndexedType() {
      cache.put(1, new NotIndexed("testing 123"));

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<NotIndexed> query = qf.create("FROM sample_bank_account.NotIndexed WHERE notIndexedField LIKE '%123%'");
      List<NotIndexed> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals("testing 123", list.get(0).notIndexedField);
   }

   public void testRemoteQueryForEmbeddedEntryOnIndexedAndNonIndexedField() {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setGender(User.Gender.MALE);
      user.setNotes("1234567890");
      cache.put(1, user);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> query = qf.create("FROM sample_bank_account.User WHERE notes LIKE '%567%' AND surname = 'test surname'");
      List<User> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals(UserPB.class, list.get(0).getClass());
      assertEquals(1, list.get(0).getId());
      assertEquals("1234567890", list.get(0).getNotes());
      assertEquals("test surname", list.get(0).getSurname());
   }

   public void testRemoteQueryWithProjectionsForEmbeddedEntry() {
      AccountHS account = new AccountHS();
      account.setId(1);
      account.setDescription("test description");
      account.setCreationDate(new Date(42));
      cache.put(1, account);

      // get account back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Object[]> query = qf.create("SELECT description, id FROM sample_bank_account.Account WHERE description LIKE '%test%'");
      List<Object[]> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals("test description", list.get(0)[0]);
      assertEquals(1, list.get(0)[1]);
   }

   public void testRemoteFullTextQuery() {
      Transaction transaction = new TransactionHS();
      transaction.setId(3);
      transaction.setDescription("Hotel");
      transaction.setLongDescription("Expenses for Infinispan F2F meeting");
      transaction.setAccountId(2);
      transaction.setAmount(99);
      transaction.setDate(new Date(42));
      transaction.setDebit(true);
      transaction.setValid(true);
      cache.put(transaction.getId(), transaction);

      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query<Transaction> q = qf.create("from sample_bank_account.Transaction where longDescription:'f2f'");

      List<Transaction> list = q.execute().list();
      assertEquals(1, list.size());
   }

   public void testEmbeddedLuceneQuery() {
      Account account = createAccountPB(1);
      remoteCache.put(1, account);

      // get account back from local cache via query and check its attributes
      QueryFactory queryFactory = org.infinispan.query.Search.getQueryFactory(embeddedCache);
      Query<Account> query = queryFactory.create(String.format("FROM %s WHERE description LIKE '%%test%%'", AccountHS.class.getName()));
      List<Account> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertAccount(list.get(0), AccountHS.class);
   }

   public void testEmbeddedQueryForEmbeddedEntryOnNonIndexedField() {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setGender(User.Gender.MALE);
      user.setNotes("1234567890");
      cache.put(1, user);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query<User> query = qf.create("FROM " + UserHS.class.getName() + " WHERE notes LIKE '%567%'");
      List<User> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals(UserHS.class, list.get(0).getClass());
      assertEquals(1, list.get(0).getId());
      assertEquals("1234567890", list.get(0).getNotes());
   }

   public void testEmbeddedQueryForEmbeddedEntryOnNonIndexedType() {
      cache.put(1, new NotIndexed("testing 123"));

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query<NotIndexed> query = qf.create("FROM " + NotIndexed.class.getName() + " WHERE notIndexedField LIKE '%123%'");
      List<NotIndexed> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals("testing 123", list.get(0).notIndexedField);
   }

   public void testEmbeddedQueryForEmbeddedEntryOnIndexedAndNonIndexedField() {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setGender(User.Gender.MALE);
      user.setNotes("1234567890");
      cache.put(1, user);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query<User> query = qf.create("FROM " + UserHS.class.getName() + " WHERE notes LIKE '%567%' AND surname = 'test surname'");
      List<User> list = query.execute().list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals(UserHS.class, list.get(0).getClass());
      assertEquals(1, list.get(0).getId());
      assertEquals("1234567890", list.get(0).getNotes());
      assertEquals("test surname", list.get(0).getSurname());
   }

   public void testIterationForRemote() {
      IntStream.range(0, 10).forEach(id -> remoteCache.put(id, createAccountPB(id)));

      // Remote unfiltered iteration
      CloseableIterator<Map.Entry<Object, Object>> remoteUnfilteredIterator = remoteCache.retrieveEntries(null, null, 10);
      remoteUnfilteredIterator.forEachRemaining(e -> {
         Integer key = (Integer) e.getKey();
         AccountPB value = (AccountPB) e.getValue();
         assertTrue(key < 10);
         assertEquals((int) key, value.getId());
      });

      // Remote filtered iteration
      KeyValueFilterConverterFactory<Integer, Account, String> filterConverterFactory = () ->
            new AbstractKeyValueFilterConverter<Integer, Account, String>() {
               @Override
               public String filterAndConvert(Integer key, Account value, Metadata metadata) {
                  if (key % 2 == 0) {
                     return value.toString();
                  }
                  return null;
               }
            };

      hotRodServer.addKeyValueFilterConverterFactory("filterConverterFactory", filterConverterFactory);

      CloseableIterator<Map.Entry<Object, Object>> remoteFilteredIterator = remoteCache.retrieveEntries("filterConverterFactory", null, 10);
      remoteFilteredIterator.forEachRemaining(e -> {
         Integer key = (Integer) e.getKey();
         String value = (String) e.getValue();
         assertTrue(key < 10);
         assertEquals(createAccountHS(key).toString(), value);
      });

      // Embedded  iteration
      Cache<Integer, AccountHS> ourCache = (Cache<Integer, AccountHS>) embeddedCache;

      Iterator<Map.Entry<Integer, AccountHS>> localUnfilteredIterator = ourCache.entrySet().stream().iterator();
      localUnfilteredIterator.forEachRemaining(e -> {
         Integer key = e.getKey();
         AccountHS value = e.getValue();
         assertTrue(key < 10);
         assertEquals((int) key, value.getId());
      });
   }

   public void testEqEmptyStringRemote() {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setGender(User.Gender.MALE);
      user.setNotes("1234567890");
      cache.put(1, user);

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<User> q = qf.create("FROM sample_bank_account.User WHERE name = ''");

      List<User> list = q.execute().list();
      assertTrue(list.isEmpty());
   }

   public void testEqSentenceRemote() {
      AccountHS account = new AccountHS();
      account.setId(1);
      account.setDescription("John Doe's first bank account");
      account.setCreationDate(new Date(42));
      cache.put(1, account);

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Account> q = qf.create("FROM sample_bank_account.Account WHERE description = \"John Doe's first bank account\"");

      List<Account> list = q.execute().list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testEqEmptyStringEmbedded() {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setGender(User.Gender.MALE);
      user.setNotes("1234567890");
      cache.put(1, user);

      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query<User> q = qf.create("FROM " + UserHS.class.getName() + " WHERE name = ''");

      List<User> list = q.execute().list();
      assertTrue(list.isEmpty());
   }

   public void testEqSentenceEmbedded() {
      AccountHS account = new AccountHS();
      account.setId(1);
      account.setDescription("John Doe's first bank account");
      account.setCreationDate(new Date(42));
      cache.put(1, account);

      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query<Account> q = qf.create("FROM " + AccountHS.class.getName() + " WHERE description = \"John Doe's first bank account\"");

      List<Account> list = q.execute().list();
      assertEquals(1, list.size());
      assertEquals(1, list.get(0).getId());
   }

   public void testDuplicateBooleanProjectionEmbedded() {
      Transaction transaction = new TransactionHS();
      transaction.setId(3);
      transaction.setDescription("Hotel");
      transaction.setAccountId(2);
      transaction.setAmount(45);
      transaction.setDate(new Date(42));
      transaction.setDebit(true);
      transaction.setValid(true);
      cache.put(transaction.getId(), transaction);

      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query<Object[]> q = qf.create("SELECT id, isDebit, isDebit FROM " + TransactionHS.class.getName() + " WHERE description = 'Hotel'");
      List<Object[]> list = q.execute().list();

      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(0)[0]);
      assertEquals(true, list.get(0)[1]);
      assertEquals(true, list.get(0)[2]);
   }

   public void testDuplicateBooleanProjectionRemote() {
      Transaction transaction = new TransactionHS();
      transaction.setId(3);
      transaction.setDescription("Hotel");
      transaction.setAccountId(2);
      transaction.setAmount(45);
      transaction.setDate(new Date(42));
      transaction.setDebit(true);
      transaction.setValid(true);
      cache.put(transaction.getId(), transaction);

      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query<Object[]> q = qf.create("SELECT id, isDebit, isDebit FROM sample_bank_account.Transaction WHERE description = 'Hotel'");
      List<Object[]> list = q.execute().list();

      assertEquals(1, list.size());
      assertEquals(3, list.get(0).length);
      assertEquals(3, list.get(0)[0]);
      assertEquals(true, list.get(0)[1]);
      assertEquals(true, list.get(0)[2]);
   }

   private AccountPB createAccountPB(int id) {
      AccountPB account = new AccountPB();
      account.setId(id);
      account.setDescription("test description");
      account.setCreationDate(new Date(42));
      return account;
   }

   private AccountHS createAccountHS(int id) {
      AccountHS account = new AccountHS();
      account.setId(id);
      account.setDescription("test description");
      account.setCreationDate(new Date(42));
      return account;
   }

   private void assertAccount(Account account, Class<?> cls) {
      assertNotNull(account);
      assertEquals(cls, account.getClass());
      assertEquals(1, account.getId());
      assertEquals("test description", account.getDescription());
      assertEquals(42, account.getCreationDate().getTime());
   }

   static class ServerSCI extends AbstractSerializationContextInitializer {

      ServerSCI() {
         super("sample_bank_account/bank.proto");
      }

      @Override
      public void registerSchema(SerializationContext ctx) {
         super.registerSchema(ctx);
         NotIndexedSCI.INSTANCE.registerSchema(ctx);
      }

      @Override
      public void registerMarshallers(SerializationContext ctx) {
         ctx.registerMarshaller(new EmbeddedAccountMarshaller());
         ctx.registerMarshaller(new CurrencyMarshaller());
         ctx.registerMarshaller(new EmbeddedLimitsMarshaller());
         ctx.registerMarshaller(new EmbeddedUserMarshaller());
         ctx.registerMarshaller(new GenderMarshaller());
         ctx.registerMarshaller(new EmbeddedTransactionMarshaller());
         ctx.registerMarshaller(new NotIndexedMarshaller());
         NotIndexedSCI.INSTANCE.registerMarshallers(ctx);
      }
   }
}
