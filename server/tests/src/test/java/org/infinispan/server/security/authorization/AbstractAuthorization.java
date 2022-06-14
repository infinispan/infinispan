package org.infinispan.server.security.authorization;

import static org.infinispan.client.rest.RestResponse.ACCEPTED;
import static org.infinispan.client.rest.RestResponse.CREATED;
import static org.infinispan.client.rest.RestResponse.FORBIDDEN;
import static org.infinispan.client.rest.RestResponse.NOT_MODIFIED;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.awaitStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.infinispan.server.test.core.TestSystemPropertyNames.HOTROD_CLIENT_SASL_MECHANISM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestClusterClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.skip.SkipJunit;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.rest.resources.WeakSSEListener;
import org.infinispan.server.functional.HotRodCacheQueries;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.Test;


/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/

public abstract class AbstractAuthorization {
   public static final String BANK_PROTO = "bank.proto";
   final Map<TestUser, ConfigurationBuilder> hotRodBuilders;
   final Map<TestUser, RestClientConfigurationBuilder> restBuilders;
   final Map<String, String> bulkData;

   protected AbstractAuthorization() {
      hotRodBuilders = new HashMap<>();
      restBuilders = new HashMap<>();
      Stream.of(TestUser.values()).forEach(this::addClientBuilders);
      bulkData = new HashMap<>();
      for (int i = 0; i < 10; i++) {
         bulkData.put("k" + i, "v" + i);
      }
   }

   protected abstract InfinispanServerRule getServers();

   protected abstract InfinispanServerTestMethodRule getServerTest();

   protected void addClientBuilders(TestUser user) {
      ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
      hotRodBuilder.security().authentication()
            .saslMechanism(System.getProperty(HOTROD_CLIENT_SASL_MECHANISM, "SCRAM-SHA-1"))
            .serverName("infinispan")
            .realm("default")
            .username(user.getUser())
            .password(user.getPassword());
      hotRodBuilders.put(user, hotRodBuilder);
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      restBuilder.security().authentication()
            .mechanism("AUTO")
            .username(user.getUser())
            .password(user.getPassword());
      restBuilders.put(user, restBuilder);
   }

   @Test
   public void testHotRodAdminAndDeployerCanDoEverything() {
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCache<String, String> cache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create();
         cache.put("k", "v");
         assertEquals("v", cache.get("k"));
         cache.putAll(bulkData);
         assertEquals(11, cache.size());
         cache.getRemoteCacheManager().administration().removeCache(cache.getName());
      }
   }

   @Test
   public void testRestAdminCanDoEverything() {
      RestCacheClient adminCache = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).withCacheMode(CacheMode.DIST_SYNC).create().cache(getServerTest().getMethodName());
      sync(adminCache.put("k", "v"));
      assertEquals("v", sync(adminCache.get("k")).getBody());

      assertStatus(OK, adminCache.distribution());
   }

   @Test
   public void testRestCacheDistribution() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.MONITOR)) {
         RestCacheClient cache = getServerTest().rest().withClientConfiguration(restBuilders.get(type)).get().cache(getServerTest().getMethodName());
         assertStatus(OK, cache.distribution());
      }

      // Types with no access.
      for (TestUser type: EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN, TestUser.MONITOR))) {
         RestCacheClient cache = getServerTest().rest().withClientConfiguration(restBuilders.get(type)).get().cache(getServerTest().getMethodName());
         assertStatus(FORBIDDEN, cache.distribution());
      }
   }

   @Test
   public void testStats() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.MONITOR)) {
         RestCacheClient cache = getServerTest().rest().withClientConfiguration(restBuilders.get(type)).get().cache(getServerTest().getMethodName());
         assertStatus(OK, cache.stats());
      }

      // Types with no access.
      for (TestUser type: EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN, TestUser.MONITOR))) {
         RestCacheClient cache = getServerTest().rest().withClientConfiguration(restBuilders.get(type)).get().cache(getServerTest().getMethodName());
         assertStatus(FORBIDDEN, cache.stats());
      }
   }

   @Test
   public void testHotRodNonAdminsMustNotCreateCache() {
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.MONITOR)) {
         Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
               () -> getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
      }
   }

   @Test
   public void testRestNonAdminsMustNotCreateCache() {
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.MONITOR)) {
         Exceptions.expectException(SecurityException.class, "(?s).*403.*",
               () -> getServerTest().rest().withClientConfiguration(restBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
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

   private void testHotRodWriterCannotRead(String... explicitRoles) {
      hotRodCreateAuthzCache(explicitRoles);
      RemoteCache<String, String> writerCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.WRITER)).get();
      writerCache.put("k1", "v1");
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> writerCache.get("k1")
      );
      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.WRITER, TestUser.MONITOR, TestUser.ANONYMOUS))) {
         RemoteCache<String, String> userCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         assertEquals("v1", userCache.get("k1"));
      }
   }

   @Test
   public void testAdminCanRemoveCacheWithoutRole() {
      RemoteCache<String, String> adminCache = hotRodCreateAuthzCache("application");
      RemoteCache<String, String> appCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.APPLICATION)).get();
      appCache.put("k1", "v1");
      adminCache.getRemoteCacheManager().administration().removeCache(adminCache.getName());
   }

   @Test
   public void testRestWriterCannotReadImplicit() {
      testRestWriterCannotRead();
   }

   @Test
   public void testScriptUpload() {
      SkipJunit.skipSinceJDK(16);
      InfinispanServerTestMethodRule serverTest = getServerTest();

      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCacheManager remoteCacheManager = serverTest.hotrod().withClientConfiguration(hotRodBuilders.get(user)).createRemoteCacheManager();
         serverTest.addScript(remoteCacheManager, "scripts/test.js");
      }

      for (TestUser user : EnumSet.of(TestUser.MONITOR, TestUser.APPLICATION, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCacheManager remoteCacheManager = serverTest.hotrod().withClientConfiguration(hotRodBuilders.get(user)).createRemoteCacheManager();
         Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
                 () -> serverTest.addScript(remoteCacheManager, "scripts/test.js")
         );
      }
   }

   @Test
   public void testExecScripts() {
      SkipJunit.skipSinceJDK(16);
      InfinispanServerTestMethodRule serverTest = getServerTest();
      RemoteCache cache = serverTest.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).create();
      String scriptName = serverTest.addScript(cache.getRemoteCacheManager(), "scripts/test.js");
      Map params = new HashMap<>();
      params.put("key", "k");
      params.put("value", "v");

      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.APPLICATION, TestUser.DEPLOYER)) {
         RemoteCache cacheExec = serverTest.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         cacheExec.execute(scriptName, params);
      }

      for (TestUser user : EnumSet.of(TestUser.MONITOR, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCache cacheExec = serverTest.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
                 () -> {
                    cacheExec.execute(scriptName, params);
                 }
         );
      }
   }

   @Test
   public void testRestWriterCannotReadExplicit() {
      testRestWriterCannotRead("admin", "observer", "deployer", "application", "writer", "reader");
   }

   private void testRestWriterCannotRead(String... explicitRoles) {
      restCreateAuthzCache(explicitRoles);
      RestCacheClient writerCache = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.WRITER)).get().cache(getServerTest().getMethodName());
      sync(writerCache.put("k1", "v1"));
      assertStatus(FORBIDDEN, writerCache.get("k1"));
      for (TestUser user : EnumSet.of(TestUser.OBSERVER, TestUser.DEPLOYER)) {
         RestCacheClient userCache = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(getServerTest().getMethodName());
         assertEquals("v1", sync(userCache.get("k1")).getBody());
      }
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
      RemoteCache<String, String> readerCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.OBSERVER)).get();
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> readerCache.put("k1", "v1")
      );
      for (TestUser user : EnumSet.of(TestUser.DEPLOYER, TestUser.APPLICATION, TestUser.WRITER)) {
         RemoteCache<String, String> userCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         userCache.put(user.name(), user.name());
      }
   }

   @Test
   public void testRestReaderCannotWriteImplicit() {
      testRestReaderCannotWrite();
   }

   @Test
   public void testRestReaderCannotWriteExplicit() {
      testRestReaderCannotWrite("admin", "observer", "deployer", "application", "writer", "reader");
   }

   private void testRestReaderCannotWrite(String... explicitRoles) {
      restCreateAuthzCache(explicitRoles);
      RestCacheClient readerCache = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.OBSERVER)).get().cache(getServerTest().getMethodName());
      assertStatus(FORBIDDEN, readerCache.put("k1", "v1"));
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.DEPLOYER)) {
         RestCacheClient userCache = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(getServerTest().getMethodName());
         assertStatus(NO_CONTENT, userCache.put(user.name(), user.name()));
      }
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
      hotRodCreateAuthzCache(explicitRoles).putAll(bulkData);
      RemoteCache<String, String> readerCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.READER)).get();
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> readerCache.getAll(bulkData.keySet())
      );
      //make sure iterator() is invoked (ISPN-12716)
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> new ArrayList<>(readerCache.keySet())
      );
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> new ArrayList<>(readerCache.values())
      );
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> new ArrayList<>(readerCache.entrySet())
      );

      RemoteCache<String, String> supervisorCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.DEPLOYER)).get();
      supervisorCache.getAll(bulkData.keySet());
      //make sure iterator() is invoked (ISPN-12716)
      assertFalse(new HashSet<>(supervisorCache.keySet()).isEmpty());
      assertFalse(new HashSet<>(supervisorCache.values()).isEmpty());
      assertFalse(new HashSet<>(supervisorCache.entrySet()).isEmpty());
   }

   @Test
   public void testAdminAndDeployerCanManageSchema() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCacheManager remoteCacheManager = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).createRemoteCacheManager();
         RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
         metadataCache.put(BANK_PROTO, schema);
         metadataCache.remove(BANK_PROTO);
      }
   }

   @Test
   public void testNonCreatorsSchema() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCacheManager remoteCacheManager = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).createRemoteCacheManager();
         RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
         Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*", () -> metadataCache.put(BANK_PROTO, schema));
      }
   }

   @Test
   public void testBulkReadUsersCanQuery() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = prepareIndexedCache();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER, TestUser.APPLICATION, TestUser.OBSERVER)) {
         RemoteCache<Integer, User> userCache = getServerTest().hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(user)).withServerConfiguration(builder).get();
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
         RestCacheClient userCache = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(getServerTest().getMethodName());
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
         RemoteCache<Integer, User> userCache = getServerTest().hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(user)).withServerConfiguration(builder).get();
         QueryFactory qf = Search.getQueryFactory(userCache);
         Query<User> query = qf.create("FROM sample_bank_account.User WHERE name = 'Tom'");
         Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*", () -> query.execute().list());
      }
      // REST
      for (TestUser user : EnumSet.of(TestUser.READER, TestUser.WRITER, TestUser.MONITOR)) {
         RestCacheClient userCache = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(getServerTest().getMethodName());
         assertStatus(FORBIDDEN, userCache.query("FROM sample_bank_account.User WHERE name = 'Tom'"));
         assertStatus(OK, userCache.searchStats());
         assertStatus(OK, userCache.indexStats());
         assertStatus(OK, userCache.queryStats());
      }
   }

   private org.infinispan.configuration.cache.ConfigurationBuilder prepareIndexedCache() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      RemoteCacheManager remoteCacheManager = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).createRemoteCacheManager();
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(BANK_PROTO, schema);

      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder
            .clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true)
            .security().authorization().enable()
            .indexing().enable().storage(LOCAL_HEAP).addIndexedEntity("sample_bank_account.User");

      RemoteCache<Integer, User> adminCache = getServerTest().hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(TestUser.ADMIN)).withServerConfiguration(builder).create();
      adminCache.put(1, HotRodCacheQueries.createUser1());
      adminCache.put(2, HotRodCacheQueries.createUser2());
      return builder;
   }

   private ConfigurationBuilder clientConfigurationWithProtostreamMarshaller(TestUser user) {
      ConfigurationBuilder client = new ConfigurationBuilder().read(hotRodBuilders.get(user).build());
      client.servers().clear();
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();
      client.marshaller(marshaller);
      Exceptions.unchecked(() -> MarshallerRegistration.registerMarshallers(marshaller.getSerializationContext()));
      return client;
   }

   @Test
   public void testAnonymousHealthPredefinedCache() {
      RestClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ANONYMOUS)).get();
      assertEquals("HEALTHY", sync(client.cacheManager("default").healthStatus()).getBody());
   }

   @Test
   public void testRestNonAdminsMustNotShutdownServer() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().stop());
      }
   }

   @Test
   public void testRestNonAdminsMustNotShutdownCluster() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().stop());
      }
   }

   @Test
   public void testRestNonAdminsMustNotModifyCacheIgnores() {
      for (TestUser user : TestUser.NON_ADMINS) {
         RestClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get();
         assertStatus(FORBIDDEN, client.server().ignoreCache("default", "predefined"));
         assertStatus(FORBIDDEN, client.server().unIgnoreCache("default", "predefined"));
      }
   }

   @Test
   public void testRestAdminsShouldBeAbleToModifyLoggers() {
      RestClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get();
      assertStatus(NO_CONTENT, client.server().logging().setLogger("org.infinispan.TEST_LOGGER", "ERROR", "STDOUT"));
      assertStatus(NO_CONTENT, client.server().logging().removeLogger("org.infinispan.TEST_LOGGER"));
   }

   @Test
   public void testRestNonAdminsMustNotModifyLoggers() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().logging().setLogger("org.infinispan.TEST_LOGGER", "ERROR", "STDOUT"));
         assertStatus(FORBIDDEN, getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().logging().removeLogger("org.infinispan.TEST_LOGGER"));
      }
   }

   @Test
   public void testRestAdminsShouldBeAbleToAdminServer() {
      RestClientConfigurationBuilder adminConfig = restBuilders.get(TestUser.ADMIN);
      RestClient client = getServerTest().rest().withClientConfiguration(adminConfig).get();
      assertStatus(NO_CONTENT, client.server().connectorStop("endpoint-alternate-1"));
      assertStatus(NO_CONTENT, client.server().connectorStart("endpoint-alternate-1"));
      assertStatus(NO_CONTENT, client.server().connectorIpFilterSet("endpoint-alternate-1", Collections.emptyList()));
      assertStatus(NO_CONTENT, client.server().connectorIpFiltersClear("endpoint-alternate-1"));
      assertStatus(OK, client.server().memory());
      assertStatus(OK, client.server().env());
      assertStatus(OK, client.server().configuration());
   }

   @Test
   public void testRestNonAdminsMustNotAdminServer() {
      for (TestUser user : TestUser.NON_ADMINS) {
         RestClientConfigurationBuilder userConfig = restBuilders.get(user);
         RestClient client = getServerTest().rest().withClientConfiguration(userConfig).get();
         assertStatus(FORBIDDEN, client.server().report());
         assertStatus(FORBIDDEN, client.server().connectorStop("endpoint-default"));
         assertStatus(FORBIDDEN, client.server().connectorStart("endpoint-default"));
         assertStatus(FORBIDDEN, client.server().connectorIpFilterSet("endpoint-default", Collections.emptyList()));
         assertStatus(FORBIDDEN, client.server().connectorIpFiltersClear("endpoint-default"));
         assertStatus(FORBIDDEN, client.server().memory());
         assertStatus(FORBIDDEN, client.server().env());
         assertStatus(FORBIDDEN, client.server().configuration());
      }
   }

   @Test
   public void testAdminsAccessToPerformXSiteOps() {
      assertXSiteOps(TestUser.ADMIN, OK, NO_CONTENT, NOT_MODIFIED);
   }

   @Test
   public void testRestNonAdminsMustNotAccessPerformXSiteOps() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertXSiteOps(user, FORBIDDEN, FORBIDDEN, FORBIDDEN);
      }
   }

   private void assertXSiteOps(TestUser user, int status, int noContentStatus, int notModifiedStatus) {
      RestClientConfigurationBuilder userConfig = restBuilders.get(user);
      RestClient client = getServerTest().rest().withClientConfiguration(userConfig).get();
      RestCacheClient xsiteCache = client.cache("xsite");
      assertStatus(status, xsiteCache.takeSiteOffline("NYC"));
      assertStatus(status, xsiteCache.bringSiteOnline("NYC"));
      assertStatus(status, xsiteCache.cancelPushState("NYC"));
      assertStatus(status, xsiteCache.cancelReceiveState("NYC"));
      assertStatus(status, xsiteCache.clearPushStateStatus());
      assertStatus(status, xsiteCache.pushSiteState("NYC"));
      assertStatus(status, xsiteCache.pushStateStatus());
      assertStatus(status, xsiteCache.xsiteBackups());
      assertStatus(status, xsiteCache.backupStatus("NYC"));
      assertStatus(status, xsiteCache.getXSiteTakeOfflineConfig("NYC"));
      assertStatus(noContentStatus, xsiteCache.updateXSiteTakeOfflineConfig("NYC", 10, 1000));
      assertStatus(status, xsiteCache.xSiteStateTransferMode("NYC"));
      assertStatus(notModifiedStatus, xsiteCache.xSiteStateTransferMode("NYC", XSiteStateTransferMode.MANUAL));
      RestCacheManagerClient xsiteCacheManager = client.cacheManager("default");
      assertStatus(status, xsiteCacheManager.bringBackupOnline("NYC"));
      assertStatus(status, xsiteCacheManager.takeOffline("NYC"));
      assertStatus(status, xsiteCacheManager.backupStatuses());
   }

   @Test
   public void testRestNonAdminsMustNotPerformSearchActions() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      assertStatus(OK, getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get().schemas().put(BANK_PROTO, schema));
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.indexing().enable().addIndexedEntity("sample_bank_account.User").statistics().enable();
      RestClient restClient = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN))
            .withServerConfiguration(builder).create();
      String indexedCache = getServerTest().getMethodName();
      RestCacheClient cache = restClient.cache(indexedCache);
      for (TestUser user : TestUser.NON_ADMINS) {
         searchActions(user, indexedCache, FORBIDDEN, FORBIDDEN);
      }
      searchActions(TestUser.ADMIN, indexedCache, OK, NO_CONTENT);
   }

   private void searchActions(TestUser user, String indexedCache, int status, int noContentStatus) {
      RestClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get();
      assertStatus(status, client.cache(indexedCache).clearSearchStats());
      assertStatus(noContentStatus, client.cache(indexedCache).reindex());
      assertStatus(noContentStatus, client.cache(indexedCache).clearIndex());
   }

   @Test
   public void testRestClusterDistributionPermission() {
      EnumSet<TestUser> allowed = EnumSet.of(TestUser.ADMIN, TestUser.MONITOR);
      for (TestUser user : allowed) {
         RestClusterClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster();
         assertStatus(OK, client.distribution());
      }

      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, allowed.toArray(new TestUser[0])))) {
         RestClusterClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster();
         assertStatus(FORBIDDEN, client.distribution());
      }
   }

   @Test
   public void testRestAdminsMustAccessBackupsAndRestores() {
      String BACKUP_NAME = "backup";
      RestClusterClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get().cluster();
      assertStatus(ACCEPTED, client.createBackup(BACKUP_NAME));
      File zip = awaitStatus(() -> client.getBackup(BACKUP_NAME, false), ACCEPTED, OK, response -> {
         String fileName = response.getHeader("Content-Disposition").split("=")[1];
         File backupZip = new File(getServers().getServerDriver().getRootDir(), fileName);
         try (InputStream is = response.getBodyAsStream()) {
            Files.copy(is, backupZip.toPath(), StandardCopyOption.REPLACE_EXISTING);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
         return backupZip;
      });
      assertStatus(NO_CONTENT, client.deleteBackup(BACKUP_NAME));
      assertStatus(OK, client.getBackupNames());
      assertStatus(ACCEPTED, client.restore(BACKUP_NAME, zip));
      assertStatus(OK, client.getRestoreNames());
      awaitStatus(() -> client.getRestore(BACKUP_NAME), ACCEPTED, CREATED);
      assertStatus(NO_CONTENT, client.deleteRestore(BACKUP_NAME));
   }

   @Test
   public void testRestNonAdminsMustNotAccessBackupsAndRestores() {
      for (TestUser user : TestUser.NON_ADMINS) {
         RestClusterClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster();
         assertStatus(FORBIDDEN, client.createBackup("backup"));
         assertStatus(FORBIDDEN, client.getBackup("backup", true));
         assertStatus(FORBIDDEN, client.getBackupNames());
         assertStatus(FORBIDDEN, client.deleteBackup("backup"));
         assertStatus(FORBIDDEN, client.restore("restore", "somewhere"));
         assertStatus(FORBIDDEN, client.getRestoreNames());
         assertStatus(FORBIDDEN, client.getRestore("restore"));
         assertStatus(FORBIDDEN, client.deleteRestore("restore"));
      }
   }

   @Test
   public void testRestListenSSEAuthorizations() throws Exception {
      RestClient adminClient = getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get();
      WeakSSEListener sseListener = new WeakSSEListener();

      // admin must be able to listen events.
      try (Closeable ignored = adminClient.raw().listen("/rest/v2/container?action=listen", Collections.emptyMap(), sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));
      }

      // non-admins must receive 403 status code.
      for (TestUser nonAdmin : TestUser.NON_ADMINS) {
         CountDownLatch latch = new CountDownLatch(1);
         RestClient client = getServerTest().rest().withClientConfiguration(restBuilders.get(nonAdmin)).get();
         WeakSSEListener listener = new WeakSSEListener() {
            @Override
            public void onError(Throwable t, RestResponse response) {
               if (response.getStatus() == FORBIDDEN) {
                  latch.countDown();
               }
            }
         };

         try (Closeable ignored = client.raw().listen("/rest/v2/container?action=listen", Collections.emptyMap(), listener)) {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
         }
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
      return getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).withServerConfiguration(builder).create();
   }

   private RestClient restCreateAuthzCache(String... explicitRoles) {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      AuthorizationConfigurationBuilder authorizationConfigurationBuilder = builder.clustering().cacheMode(CacheMode.DIST_SYNC).security().authorization().enable();
      if (explicitRoles != null) {
         for (String role : explicitRoles) {
            authorizationConfigurationBuilder.role(role);
         }
      }
      return getServerTest().rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).withServerConfiguration(builder).create();
   }
}
