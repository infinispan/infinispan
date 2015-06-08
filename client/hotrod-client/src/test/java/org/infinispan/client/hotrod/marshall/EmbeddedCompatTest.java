package org.infinispan.client.hotrod.marshall;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AccountPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.NotIndexedMarshaller;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Index;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.filter.AbstractKeyValueFilterConverter;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Account;
import org.infinispan.query.dsl.embedded.testdomain.NotIndexed;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.AccountHS;
import org.infinispan.query.dsl.embedded.testdomain.hsearch.UserHS;
import org.infinispan.query.remote.CompatibilityProtoStreamMarshaller;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.*;

/**
 * Tests compatibility between remote query and embedded mode.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.marshall.EmbeddedCompatTest", groups = "functional")
@CleanupAfterMethod
public class EmbeddedCompatTest extends SingleCacheManagerTest {

   private static final String NOT_INDEXED_PROTO_SCHEMA = "package sample_bank_account;\n" +
         "/* @Indexed(false) */\n" +
         "message NotIndexed {\n" +
         "\toptional string notIndexedField = 1;\n" +
         "}\n";

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, Account> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = createConfigBuilder();
      // the default key equivalence works only for byte[] so we need to override it with one that works for Object
      builder.dataContainer().keyEquivalence(AnyEquivalence.getInstance());

      cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      cache = cacheManager.getCache();

      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache();

      //initialize client-side serialization context
      SerializationContext clientSerCtx = ProtoStreamMarshaller.getSerializationContext(remoteCacheManager);
      MarshallerRegistration.registerMarshallers(clientSerCtx);
      clientSerCtx.registerProtoFiles(FileDescriptorSource.fromString("not_indexed.proto", NOT_INDEXED_PROTO_SCHEMA));
      clientSerCtx.registerMarshaller(new NotIndexedMarshaller());

      //initialize server-side serialization context
      ProtobufMetadataManager protobufMetadataManager = cacheManager.getGlobalComponentRegistry().getComponent(ProtobufMetadataManager.class);
      protobufMetadataManager.registerProtofile("sample_bank_account/bank.proto", Util.read(Util.getResourceAsStream("/sample_bank_account/bank.proto", getClass().getClassLoader())));
      protobufMetadataManager.registerProtofile("not_indexed.proto", NOT_INDEXED_PROTO_SCHEMA);
      assertNull(protobufMetadataManager.getFileErrors("sample_bank_account/bank.proto"));
      assertNull(protobufMetadataManager.getFileErrors("not_indexed.proto"));
      assertNull(protobufMetadataManager.getFilesWithErrors());

      protobufMetadataManager.registerMarshaller(new EmbeddedAccountMarshaller());
      protobufMetadataManager.registerMarshaller(new EmbeddedUserMarshaller());
      protobufMetadataManager.registerMarshaller(new NotIndexedMarshaller());

      return cacheManager;
   }

   protected org.infinispan.configuration.cache.ConfigurationBuilder createConfigBuilder() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = hotRodCacheConfiguration();
      builder.compatibility().enable().marshaller(new CompatibilityProtoStreamMarshaller());
      builder.indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return builder;
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testPutAndGet() throws Exception {
      Account account = createAccountPB(1);
      remoteCache.put(1, account);

      // try to get the object through the local cache interface and check it's the same object we put
      assertEquals(1, cache.keySet().size());
      Object key = cache.keySet().iterator().next();
      Object localObject = cache.get(key);
      assertAccount((Account) localObject, AccountHS.class);

      // get the object through the remote cache interface and check it's the same object we put
      Account fromRemoteCache = remoteCache.get(1);
      assertAccount(fromRemoteCache, AccountPB.class);
   }

   public void testPutAndGetForEmbeddedEntry() throws Exception {
      AccountHS account = new AccountHS();
      account.setId(1);
      account.setDescription("test description");
      account.setCreationDate(new Date(42));
      cache.put(1, account);

      // try to get the object through the local cache interface and check it's the same object we put
      assertEquals(1, remoteCache.keySet().size());
      Object key = remoteCache.keySet().iterator().next();
      Object remoteObject = remoteCache.get(key);
      assertAccount((Account) remoteObject, AccountPB.class);

      // get the object through the embedded cache interface and check it's the same object we put
      Account fromEmbeddedCache = (Account) cache.get(1);
      assertAccount(fromEmbeddedCache, AccountHS.class);
   }

   public void testRemoteQuery() throws Exception {
      Account account = createAccountPB(1);
      remoteCache.put(1, account);

      // get account back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(AccountPB.class)
            .having("description").like("%test%").toBuilder()
            .build();
      List<Account> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertAccount(list.get(0), AccountPB.class);
   }

   public void testRemoteQueryForEmbeddedEntry() throws Exception {
      AccountHS account = new AccountHS();
      account.setId(1);
      account.setDescription("test description");
      account.setCreationDate(new Date(42));
      cache.put(1, account);

      // get account back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(AccountPB.class)
            .having("description").like("%test%").toBuilder()
            .build();
      List<Account> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertAccount(list.get(0), AccountPB.class);
   }

   public void testRemoteQueryForEmbeddedEntryOnNonIndexedField() throws Exception {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setNotes("1234567890");
      cache.put(1, user);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .having("notes").like("%567%").toBuilder()
            .build();
      List<User> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals(UserPB.class, list.get(0).getClass());
      assertEquals(1, list.get(0).getId());
      assertEquals("1234567890", list.get(0).getNotes());
   }

   public void testRemoteQueryForEmbeddedEntryOnNonIndexedType() throws Exception {
      cache.put(1, new NotIndexed("testing 123"));

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from("sample_bank_account.NotIndexed")
            .having("notIndexedField").like("%123%").toBuilder()
            .build();
      List<NotIndexed> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals("testing 123", list.get(0).notIndexedField);
   }

   public void testRemoteQueryForEmbeddedEntryOnIndexedAndNonIndexedField() throws Exception {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setNotes("1234567890");
      cache.put(1, user);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .having("notes").like("%567%")
            .and()
            .having("surname").eq("test surname")
            .toBuilder()
            .build();
      List<User> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals(UserPB.class, list.get(0).getClass());
      assertEquals(1, list.get(0).getId());
      assertEquals("1234567890", list.get(0).getNotes());
      assertEquals("test surname", list.get(0).getSurname());
   }

   public void testRemoteQueryWithProjectionsForEmbeddedEntry() throws Exception {
      AccountHS account = new AccountHS();
      account.setId(1);
      account.setDescription("test description");
      account.setCreationDate(new Date(42));
      cache.put(1, account);

      // get account back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(AccountPB.class)
            .setProjection("description", "id")
            .having("description").like("%test%").toBuilder()
            .build();
      List<Object[]> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals("test description", list.get(0)[0]);
      assertEquals(1, list.get(0)[1]);
   }

   public void testEmbeddedLuceneQuery() throws Exception {
      Account account = createAccountPB(1);
      remoteCache.put(1, account);

      // get account back from local cache via query and check its attributes
      SearchManager searchManager = org.infinispan.query.Search.getSearchManager(cache);
      org.apache.lucene.search.Query query = searchManager
            .buildQueryBuilderForClass(AccountHS.class).get()
            .keyword().wildcard().onField("description").matching("*test*").createQuery();
      CacheQuery cacheQuery = searchManager.getQuery(query);
      List<Object> list = cacheQuery.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertAccount((Account) list.get(0), AccountHS.class);
   }

   public void testEmbeddedQueryForEmbeddedEntryOnNonIndexedField() throws Exception {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setNotes("1234567890");
      cache.put(1, user);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query query = qf.from(UserHS.class)
            .having("notes").like("%567%").toBuilder()
            .build();
      List<User> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals(UserHS.class, list.get(0).getClass());
      assertEquals(1, list.get(0).getId());
      assertEquals("1234567890", list.get(0).getNotes());
   }

   public void testEmbeddedQueryForEmbeddedEntryOnNonIndexedType() throws Exception {
      cache.put(1, new NotIndexed("testing 123"));

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query query = qf.from(NotIndexed.class)
            .having("notIndexedField").like("%123%").toBuilder()
            .build();
      List<NotIndexed> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals("testing 123", list.get(0).notIndexedField);
   }

   public void testEmbeddedQueryForEmbeddedEntryOnIndexedAndNonIndexedField() throws Exception {
      UserHS user = new UserHS();
      user.setId(1);
      user.setName("test name");
      user.setSurname("test surname");
      user.setNotes("1234567890");
      cache.put(1, user);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = org.infinispan.query.Search.getQueryFactory(cache);
      Query query = qf.from(UserHS.class)
            .having("notes").like("%567%")
            .and()
            .having("surname").eq("test surname")
            .toBuilder()
            .build();
      List<User> list = query.list();

      assertNotNull(list);
      assertEquals(1, list.size());
      assertNotNull(list.get(0));
      assertEquals(UserHS.class, list.get(0).getClass());
      assertEquals(1, list.get(0).getId());
      assertEquals("1234567890", list.get(0).getNotes());
      assertEquals("test surname", list.get(0).getSurname());
   }

   public void testIterationForRemote() throws Exception {
      IntStream.range(0, 10).forEach(id -> remoteCache.put(id, createAccountPB(id)));

      // Remote unfiltered iteration
      CloseableIterator<Map.Entry<Object, Object>> remoteUnfilteredIterator = remoteCache.retrieveEntries(null, null, 10);
      remoteUnfilteredIterator.forEachRemaining(e -> {
         Integer key = (Integer) e.getKey();
         AccountPB value = (AccountPB) e.getValue();
         assertTrue(key < 10);
         assertTrue(value.getId() == key);
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
         assertTrue(value.equals(createAccountHS(key).toString()));
      });

      // Embedded  iteration
      EntryRetriever<Integer, Account> localRetriever = cache.getAdvancedCache().getComponentRegistry().getComponent(EntryRetriever.class);
      CloseableIterator<CacheEntry<Integer, AccountHS>> localUnfilteredIterator = localRetriever.retrieveEntries(null, null, null, null);
      localUnfilteredIterator.forEachRemaining(e -> {
         Integer key = e.getKey();
         AccountHS value = e.getValue();
         assertTrue(key < 10);
         assertTrue(value.getId() == key);
      });
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
}
