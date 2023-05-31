package org.infinispan.server.security.authorization;

import static org.infinispan.client.rest.RestResponse.FORBIDDEN;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.skip.SkipJunit;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.functional.hotrod.HotRodCacheQueries;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Security.class)
public class HotRodAuthorizationTest extends BaseTest {

   @Test
   public void testHotRodAdminAndDeployerCanDoEverything() {
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCache<String, String> cache = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create();
         cache.put("k", "v");
         assertEquals("v", cache.get("k"));
         cache.putAll(suite.bulkData);
         assertEquals(11, cache.size());
         cache.getRemoteCacheManager().administration().removeCache(cache.getName());
      }
   }

   @Test
   public void testHotRodNonAdminsMustNotCreateCache() {
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.MONITOR)) {
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
      }
   }

   @Test
   public void testHotRodWriterCannotReadImplicit() {
      testHotRodWriterCannotRead();
   }

   @Test
   public void testHotRodWriterCannotReadExplicit() {
      testHotRodWriterCannotRead("admin", "observer", "deployer", "application", "writer", "reader", "monitor");
   }

   @Test
   public void testHotRodBulkOperationsImplicit() {
      testHotRodBulkOperations();
   }

   @Test
   public void testHotRodBulkOperationsExplicit() {
      testHotRodBulkOperations("admin", "observer", "deployer", "application", "writer", "reader");
   }

   private void testHotRodBulkOperations(String... explicitRoles) {
      hotRodCreateAuthzCache(explicitRoles).putAll(suite.bulkData);
      RemoteCache<String, String> readerCache = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.READER)).get();
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> readerCache.getAll(suite.bulkData.keySet())
      );
      //make sure iterator() is invoked (ISPN-12716)
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> new ArrayList<>(readerCache.keySet())
      );
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> new ArrayList<>(readerCache.values())
      );
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> new ArrayList<>(readerCache.entrySet())
      );

      RemoteCache<String, String> supervisorCache = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.DEPLOYER)).get();
      supervisorCache.getAll(suite.bulkData.keySet());
      //make sure iterator() is invoked (ISPN-12716)
      assertFalse(new HashSet<>(supervisorCache.keySet()).isEmpty());
      assertFalse(new HashSet<>(supervisorCache.values()).isEmpty());
      assertFalse(new HashSet<>(supervisorCache.entrySet()).isEmpty());
   }

   @Test
   public void testHotRodReaderCannotWriteImplicit() {
      testHotRodObserverCannotWrite();
   }

   @Test
   public void testHotRodReaderCannotWriteExplicit() {
      testHotRodObserverCannotWrite();
   }

   private void testHotRodObserverCannotWrite(String... explicitRoles) {
      hotRodCreateAuthzCache(explicitRoles);
      RemoteCache<String, String> readerCache = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.OBSERVER)).get();
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> readerCache.put("k1", "v1")
      );
      for (TestUser user : EnumSet.of(TestUser.DEPLOYER, TestUser.APPLICATION, TestUser.WRITER)) {
         RemoteCache<String, String> userCache = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         userCache.put(user.name(), user.name());
      }
   }

   @Test
   public void testScriptUpload() {
      SkipJunit.skipCondition(() -> suite.getServers().getServerDriver().getConfiguration().runMode() != ServerRunMode.CONTAINER);
      InfinispanServerTestMethodRule serverTest = suite.getServerTest();

      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCacheManager remoteCacheManager = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).createRemoteCacheManager();
         serverTest.addScript(remoteCacheManager, "scripts/test.js");
      }

      for (TestUser user : EnumSet.of(TestUser.MONITOR, TestUser.APPLICATION, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCacheManager remoteCacheManager = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).createRemoteCacheManager();
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> serverTest.addScript(remoteCacheManager, "scripts/test.js")
         );
      }
   }

   @Test
   public void testAdminCanRemoveCacheWithoutRole() {
      RemoteCache<String, String> adminCache = hotRodCreateAuthzCache("application");
      RemoteCache<String, String> appCache = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.APPLICATION)).get();
      appCache.put("k1", "v1");
      adminCache.getRemoteCacheManager().administration().removeCache(adminCache.getName());
   }

   @Test
   public void testExecScripts() {
      SkipJunit.skipCondition(() -> suite.getServers().getServerDriver().getConfiguration().runMode() != ServerRunMode.CONTAINER);
      InfinispanServerTestMethodRule serverTest = suite.getServerTest();
      RemoteCache cache = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.ADMIN)).create();
      String scriptName = serverTest.addScript(cache.getRemoteCacheManager(), "scripts/test.js");
      Map params = new HashMap<>();
      params.put("key", "k");
      params.put("value", "v");

      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.APPLICATION, TestUser.DEPLOYER)) {
         RemoteCache cacheExec = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         cacheExec.execute(scriptName, params);
      }

      for (TestUser user : EnumSet.of(TestUser.MONITOR, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCache cacheExec = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> {
                  cacheExec.execute(scriptName, params);
               }
         );
      }
   }

   @Test
   public void testServerTaskWithParameters() {
      if (!(suite.getServers().getServerDriver() instanceof ContainerInfinispanServerDriver)) {
         throw new AssumptionViolatedException("Requires CONTAINER mode");
      }
      InfinispanServerTestMethodRule serverTest = suite.getServerTest();
      serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.ADMIN)).create();

      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.APPLICATION, TestUser.DEPLOYER)) {
         RemoteCache<String, String> cache = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         ArrayList<String> messages = cache.execute("hello", Collections.singletonMap("greetee", new ArrayList<>(Arrays.asList("nurse", "kitty"))));
         assertEquals(2, messages.size());
         assertEquals("Hello nurse", messages.get(0));
         assertEquals("Hello kitty", messages.get(1));
         String message = cache.execute("hello", Collections.emptyMap());
         assertEquals("Hello " + expectedServerPrincipalName(user), message);
      }

      for (TestUser user : EnumSet.of(TestUser.MONITOR, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCache<String, String> cache = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> cache.execute("hello", Collections.singletonMap("greetee", new ArrayList<>(Arrays.asList("nurse", "kitty"))))
         );
      }
   }

   @Test
   public void testCacheUpdateConfigurationAttribute() {
      InfinispanServerTestMethodRule serverTest = suite.getServerTest();
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.memory().maxCount(100);
      serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.ADMIN)).withServerConfiguration(builder).create();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCache<String, String> cache = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         cache.getRemoteCacheManager().administration().updateConfigurationAttribute(cache.getName(), "memory.max-count", "1000");
      }

      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER, TestUser.ANONYMOUS))) {
         RemoteCache<String, String> cache = serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> cache.getRemoteCacheManager().administration().updateConfigurationAttribute(cache.getName(), "memory.max-count", "500")
         );
      }
   }

   @Test
   public void testDistributedServerTaskWithParameters() {
      if (!(suite.getServers().getServerDriver() instanceof ContainerInfinispanServerDriver)) {
         throw new AssumptionViolatedException("Requires CONTAINER mode");
      }
      InfinispanServerTestMethodRule serverTest = suite.getServerTest();
      serverTest.hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.ADMIN)).create();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.APPLICATION, TestUser.DEPLOYER)) {
         // We must utilise the GenericJBossMarshaller due to ISPN-8814
         RemoteCache<String, String> cache = serverTest.hotrod().withMarshaller(GenericJBossMarshaller.class).withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         List<String> greetings = cache.execute("dist-hello", Collections.singletonMap("greetee", "my friend"));
         assertEquals(2, greetings.size());
         for (String greeting : greetings) {
            assertTrue(greeting.matches("Hello my friend .*"));
         }
         greetings = cache.execute("dist-hello", Collections.emptyMap());
         assertEquals(2, greetings.size());
         for (String greeting : greetings) {
            assertTrue(greeting, greeting.startsWith("Hello " + expectedServerPrincipalName(user) + " from "));
         }
      }
   }

   @Test
   public void testAdminAndDeployerCanManageSchema() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCacheManager remoteCacheManager = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).createRemoteCacheManager();
         RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
         metadataCache.put(BANK_PROTO, schema);
         metadataCache.remove(BANK_PROTO);
      }
   }

   @Test
   public void testNonCreatorsSchema() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCacheManager remoteCacheManager = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).createRemoteCacheManager();
         RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION, () -> metadataCache.put(BANK_PROTO, schema));
      }
   }

   @Test
   public void testBulkReadUsersCanQuery() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = prepareIndexedCache();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER, TestUser.APPLICATION, TestUser.OBSERVER)) {
         RemoteCache<Integer, User> userCache = suite.getServerTest().hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(user)).withServerConfiguration(builder).get();
         User fromCache = userCache.get(1);
         HotRodCacheQueries.assertUser1(fromCache);
         QueryFactory qf = Search.getQueryFactory(userCache);
         Query<User> query = qf.create("FROM sample_bank_account.User WHERE name = 'Tom'");
         List<User> list = query.execute().list();
         assertNotNull(list);
         assertEquals(1, list.size());
         assertEquals(User.class, list.get(0).getClass());
         HotRodCacheQueries.assertUser1(list.get(0));
      }

      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER, TestUser.APPLICATION, TestUser.OBSERVER)) {
         RestCacheClient userCache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().cache(suite.getServerTest().getMethodName());
         assertStatus(OK, userCache.query("FROM sample_bank_account.User WHERE name = 'Tom'"));
         assertStatus(OK, userCache.searchStats());
         assertStatus(OK, userCache.indexStats());
         assertStatus(OK, userCache.queryStats());
      }
   }

   @Test
   public void testNonBulkReadUsersCannotQuery() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = prepareIndexedCache();
      // Hot Rod
      for (TestUser user : EnumSet.of(TestUser.READER, TestUser.WRITER)) {
         RemoteCache<Integer, User> userCache = suite.getServerTest().hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(user)).withServerConfiguration(builder).get();
         QueryFactory qf = Search.getQueryFactory(userCache);
         Query<User> query = qf.create("FROM sample_bank_account.User WHERE name = 'Tom'");
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION, () -> query.execute().list());
      }
      // REST
      for (TestUser user : EnumSet.of(TestUser.READER, TestUser.WRITER, TestUser.MONITOR)) {
         RestCacheClient userCache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().cache(suite.getServerTest().getMethodName());
         assertStatus(FORBIDDEN, userCache.query("FROM sample_bank_account.User WHERE name = 'Tom'"));
         assertStatus(OK, userCache.searchStats());
         assertStatus(OK, userCache.indexStats());
         assertStatus(OK, userCache.queryStats());
      }
   }

   @Test
   public void testHotRodCacheNames() {
      hotRodCreateAuthzCache("admin", "observer", "deployer");
      String name = suite.getServerTest().getMethodName();

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.OBSERVER, TestUser.DEPLOYER)) {
         Set<String> caches = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(type)).get().getRemoteCacheContainer().getCacheNames();
         assertTrue(caches.toString(), caches.contains(name));
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ADMIN, TestUser.OBSERVER, TestUser.DEPLOYER, TestUser.ANONYMOUS))) {
         Set<String> caches = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(type)).get().getRemoteCacheContainer().getCacheNames();
         assertFalse(caches.toString(), caches.contains(name));
      }
   }

   private org.infinispan.configuration.cache.ConfigurationBuilder prepareIndexedCache() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      RemoteCacheManager remoteCacheManager = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.ADMIN)).createRemoteCacheManager();
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(BANK_PROTO, schema);

      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder
            .clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true)
            .security().authorization().enable()
            .indexing().enable().storage(LOCAL_HEAP).addIndexedEntity("sample_bank_account.User");

      RemoteCache<Integer, User> adminCache = suite.getServerTest().hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(TestUser.ADMIN)).withServerConfiguration(builder).create();
      adminCache.put(1, HotRodCacheQueries.createUser1());
      adminCache.put(2, HotRodCacheQueries.createUser2());
      return builder;
   }

   private ConfigurationBuilder clientConfigurationWithProtostreamMarshaller(TestUser user) {
      ConfigurationBuilder client = new ConfigurationBuilder().read(suite.hotRodBuilders.get(user).build());
      client.servers().clear();
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();
      client.marshaller(marshaller);
      Exceptions.unchecked(() -> MarshallerRegistration.registerMarshallers(marshaller.getSerializationContext()));
      return client;
   }

   private void testHotRodWriterCannotRead(String... explicitRoles) {
      hotRodCreateAuthzCache(explicitRoles);
      RemoteCache<String, String> writerCache = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.WRITER)).get();
      writerCache.put("k1", "v1");
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> writerCache.get("k1")
      );
      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.WRITER, TestUser.MONITOR, TestUser.ANONYMOUS))) {
         RemoteCache<String, String> userCache = suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(user)).get();
         assertEquals("v1", userCache.get("k1"));
      }
   }

   private <K, V> RemoteCache<K, V> hotRodCreateAuthzCache(String... explicitRoles) {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      AuthorizationConfigurationBuilder authorizationConfigurationBuilder = builder.clustering().cacheMode(CacheMode.DIST_SYNC).security().authorization().enable();
      if (explicitRoles != null) {
         for (String role : explicitRoles) {
            authorizationConfigurationBuilder.role(role);
         }
      }
      return suite.getServerTest().hotrod().withClientConfiguration(suite.hotRodBuilders.get(TestUser.ADMIN)).withServerConfiguration(builder).create();
   }

   protected String expectedServerPrincipalName(TestUser user) {
      return suite.expectedServerPrincipalName(user);
   }
}
