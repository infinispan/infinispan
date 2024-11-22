package org.infinispan.server.security.authorization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestHeaders.CONTENT_DISPOSITION;
import static org.infinispan.client.rest.RestResponse.ACCEPTED;
import static org.infinispan.client.rest.RestResponse.CREATED;
import static org.infinispan.client.rest.RestResponse.FORBIDDEN;
import static org.infinispan.client.rest.RestResponse.NOT_FOUND;
import static org.infinispan.client.rest.RestResponse.NOT_MODIFIED;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.client.rest.RestResponse.TEMPORARY_REDIRECT;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.assertStatusAndBodyEquals;
import static org.infinispan.server.test.core.Common.awaitStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestClusterClient;
import org.infinispan.client.rest.RestContainerClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestResponseInfo;
import org.infinispan.client.rest.RestSecurityClient;
import org.infinispan.client.rest.RestServerClient;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.protostream.sampledomain.TestDomainSCI;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.resources.WeakSSEListener;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.junit.jupiter.api.Test;

abstract class RESTAuthorizationTest {

   public static final String BANK_PROTO = "bank.proto";

   protected final InfinispanServerExtension ext;

   protected final Function<TestUser, String> serverPrincipal;
   protected final Map<TestUser, RestClientConfigurationBuilder> restBuilders;

   public RESTAuthorizationTest(InfinispanServerExtension ext) {
      this(ext, TestUser::getUser, user -> {
         RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
         if (user.getUser() != null) {
            restBuilder.security().authentication()
                  .mechanism("AUTO")
                  .username(user.getUser())
                  .password(user.getPassword());
         }
         return restBuilder;
      });
   }

   public RESTAuthorizationTest(InfinispanServerExtension ext, Function<TestUser, String> serverPrincipal, Function<TestUser, RestClientConfigurationBuilder> restBuilder) {
      this.ext = ext;
      this.serverPrincipal = serverPrincipal;
      this.restBuilders = Stream.of(TestUser.values()).collect(Collectors.toMap(user -> user, restBuilder));
   }

   @Test
   public void testRestAdminCanDoEverything() {
      RestCacheClient adminCache = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN))
            .withCacheMode(CacheMode.DIST_SYNC).create().cache(ext.getMethodName());
      assertStatus(NO_CONTENT, adminCache.put("k", "v"));
      assertStatusAndBodyEquals(OK, "v", adminCache.get("k"));
      assertStatus(OK, adminCache.distribution());
   }

   @Test
   public void testRestLocalCacheDistribution() {
      restCreateAuthzLocalCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");
      actualTestCacheDistribution();
   }

   @Test
   public void testRestCacheDistribution() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");
      actualTestCacheDistribution();
   }

   private void actualTestCacheDistribution() {
      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.MONITOR)) {
         RestCacheClient cache = ext.rest().withClientConfiguration(restBuilders.get(type)).get()
               .cache(ext.getMethodName());
         assertStatus(OK, cache.distribution());
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN, TestUser.MONITOR))) {
         RestCacheClient cache = ext.rest().withClientConfiguration(restBuilders.get(type)).get().cache(ext.getMethodName());
         assertStatus(FORBIDDEN, cache.distribution());
      }
   }

   @Test
   public void testRestKeyDistribution() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");
      RestCacheClient adminCache = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN))
            .get().cache(ext.getMethodName());
      assertStatus(NO_CONTENT, adminCache.put("existentKey", "v"));

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.MONITOR)) {
         RestCacheClient cache = ext.rest().withClientConfiguration(restBuilders.get(type)).get()
               .cache(ext.getMethodName());
         assertStatus(OK, cache.distribution("somekey"));
         assertStatus(OK, cache.distribution("existentKey"));
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN, TestUser.MONITOR))) {
         RestCacheClient cache = ext.rest().withClientConfiguration(restBuilders.get(type)).get()
               .cache(ext.getMethodName());
         assertStatus(FORBIDDEN, cache.distribution("somekey"));
         assertStatus(FORBIDDEN, cache.distribution("existentKey"));
      }
   }

   @Test
   public void testStats() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.MONITOR)) {
         RestCacheClient cache = ext.rest().withClientConfiguration(restBuilders.get(type)).get()
               .cache(ext.getMethodName());
         assertStatus(OK, cache.stats());
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN, TestUser.MONITOR))) {
         RestCacheClient cache = ext.rest().withClientConfiguration(restBuilders.get(type)).get()
               .cache(ext.getMethodName());
         assertStatus(FORBIDDEN, cache.stats());
      }
   }

   @Test
   public void testStatsReset() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");

      for (TestUser type : EnumSet.of(TestUser.ADMIN)) {
         RestCacheClient cache = ext.rest().withClientConfiguration(restBuilders.get(type)).get()
               .cache(ext.getMethodName());
         assertStatus(NO_CONTENT, cache.statsReset());
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN))) {
         RestCacheClient cache = ext.rest().withClientConfiguration(restBuilders.get(type)).get()
               .cache(ext.getMethodName());
         assertStatus(FORBIDDEN, cache.statsReset());
      }
   }

   @Test
   public void testRestNonAdminsMustNotCreateCache() {
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.MONITOR)) {
         Exceptions.expectException(SecurityException.class, "(?s).*403.*",
               () -> ext.rest().withClientConfiguration(restBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
      }
   }

   @Test
   public void testRestNonAdminsMustNotReadCacheConfig() {
      restCreateAuthzCache();
      String name = ext.getMethodName();
      for (TestUser user : EnumSet.of(TestUser.ADMIN)) {
         RestClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get();
         assertStatus(OK, client.cache(name).configuration());
         String details = assertStatus(OK, client.cache(name).details());
         Json json = Json.read(details);
         assertNotNull(json.asJsonMap().get("configuration"));
      }
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.MONITOR)) {
         RestClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get();
         assertStatus(FORBIDDEN, client.cache(name).configuration());
         String details = assertStatus(OK, client.cache(name).details());
         Json json = Json.read(details);
         assertNull(json.asJsonMap().get("configuration"));
      }
   }

   @Test
   public void testRestWriterCannotReadImplicit() {
      testRestWriterCannotRead();
   }

   @Test
   public void testRestWriterCannotReadExplicit() {
      testRestWriterCannotRead("admin", "observer", "deployer", "application", "writer", "reader");
   }

   private void testRestWriterCannotRead(String... explicitRoles) {
      restCreateAuthzCache(explicitRoles);
      RestCacheClient writerCache = ext.rest().withClientConfiguration(restBuilders.get(TestUser.WRITER)).get()
            .cache(ext.getMethodName());
      assertStatus(NO_CONTENT, writerCache.put("k1", "v1"));
      assertStatus(FORBIDDEN, writerCache.get("k1"));
      assertStatus(FORBIDDEN, writerCache.keys());
      assertStatus(FORBIDDEN, writerCache.entries());
      for (TestUser user : EnumSet.of(TestUser.OBSERVER, TestUser.DEPLOYER)) {
         RestCacheClient userCache = ext.rest().withClientConfiguration(restBuilders.get(user)).get()
               .cache(ext.getMethodName());
         assertStatusAndBodyEquals(OK, "v1", userCache.get("k1"));
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
      RestCacheClient readerCache = ext.rest().withClientConfiguration(restBuilders.get(TestUser.OBSERVER)).get()
            .cache(ext.getMethodName());
      assertStatus(FORBIDDEN, readerCache.put("k1", "v1"));
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.DEPLOYER)) {
         RestCacheClient userCache = ext.rest().withClientConfiguration(restBuilders.get(user)).get()
               .cache(ext.getMethodName());
         assertStatus(NO_CONTENT, userCache.put(user.name(), user.name()));
      }
   }

   @Test
   public void testAnonymousHealthPredefinedCache() {
      RestClient client = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ANONYMOUS)).get();
      assertStatusAndBodyEquals(OK, "HEALTHY", client.container().healthStatus());
   }

   @Test
   public void testRestNonAdminsMustNotShutdownServer() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, ext.rest().withClientConfiguration(restBuilders.get(user)).get().server().stop());
      }
   }

   @Test
   public void testRestNonAdminsMustNotShutdownCluster() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, ext.rest().withClientConfiguration(restBuilders.get(user)).get().cluster().stop());
      }
   }

   @Test
   public void testRestNonAdminsMustNotModifyCacheIgnores() {
      for (TestUser user : TestUser.NON_ADMINS) {
         RestClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get();
         assertStatus(FORBIDDEN, client.server().ignoreCache("predefined"));
         assertStatus(FORBIDDEN, client.server().unIgnoreCache("predefined"));
      }
   }

   @Test
   public void testRestAdminsShouldBeAbleToModifyLoggers() {
      RestClient client = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get();
      assertStatus(NO_CONTENT, client.server().logging().setLogger("org.infinispan.TEST_LOGGER", "ERROR", "STDOUT"));
      assertStatus(NO_CONTENT, client.server().logging().removeLogger("org.infinispan.TEST_LOGGER"));
   }

   @Test
   public void testRestNonAdminsMustNotModifyLoggers() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, ext.rest().withClientConfiguration(restBuilders.get(user)).get().server().logging().setLogger("org.infinispan.TEST_LOGGER", "ERROR", "STDOUT"));
         assertStatus(FORBIDDEN, ext.rest().withClientConfiguration(restBuilders.get(user)).get().server().logging().removeLogger("org.infinispan.TEST_LOGGER"));
      }
   }

   @Test
   public void testRestAdminsShouldBeAbleToAdminServer() {
      RestClientConfigurationBuilder adminConfig = restBuilders.get(TestUser.ADMIN);
      RestClient client = ext.rest().withClientConfiguration(adminConfig).get();
      assertStatus(NO_CONTENT, client.server().connectorStop("endpoint-alternate-1"));
      assertStatus(NO_CONTENT, client.server().connectorStart("endpoint-alternate-1"));
      assertStatus(NO_CONTENT, client.server().connectorIpFilterSet("endpoint-alternate-1", Collections.emptyList()));
      assertStatus(NO_CONTENT, client.server().connectorIpFiltersClear("endpoint-alternate-1"));
      assertStatus(OK, client.server().memory());
      assertStatus(OK, client.server().env());
      assertStatus(OK, client.server().configuration());
      String connections = assertStatus(OK, client.server().listConnections(true));
      Json json = Json.read(connections);
      assertTrue(json.isArray());
   }

   @Test
   public void testRestNonAdminsMustNotAdminServer() {
      for (TestUser user : TestUser.NON_ADMINS) {
         RestClientConfigurationBuilder userConfig = restBuilders.get(user);
         RestClient client = ext.rest().withClientConfiguration(userConfig).get();
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
      RestClient client = ext.rest().withClientConfiguration(userConfig).get();
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
      RestContainerClient xsiteCacheManager = client.container();
      assertStatus(status, xsiteCacheManager.bringBackupOnline("NYC"));
      assertStatus(status, xsiteCacheManager.takeOffline("NYC"));
      assertStatus(status, xsiteCacheManager.backupStatuses());
   }

   @Test
   public void testRestNonAdminsMustNotPerformSearchActions() {
      String schema = TestDomainSCI.INSTANCE.getProtoFile();
      assertStatus(OK, ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get().schemas().put(BANK_PROTO, schema));
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.indexing().enable().addIndexedEntity("sample_bank_account.User").statistics().enable();
      RestClient restClient = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN))
            .withServerConfiguration(builder).create();
      String indexedCache = ext.getMethodName();
      RestCacheClient cache = restClient.cache(indexedCache);
      for (TestUser user : TestUser.NON_ADMINS) {
         searchActions(user, indexedCache, FORBIDDEN, FORBIDDEN);
      }
      searchActions(TestUser.ADMIN, indexedCache, OK, NO_CONTENT);
   }

   private void searchActions(TestUser user, String indexedCache, int status, int noContentStatus) {
      RestClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get();
      assertStatus(status, client.cache(indexedCache).clearSearchStats());
      assertStatus(noContentStatus, client.cache(indexedCache).reindex());
      assertStatus(noContentStatus, client.cache(indexedCache).clearIndex());
   }

   @Test
   public void testRestClusterDistributionPermission() {
      EnumSet<TestUser> allowed = EnumSet.of(TestUser.ADMIN, TestUser.MONITOR);
      for (TestUser user : allowed) {
         RestClusterClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get().cluster();
         assertStatus(OK, client.distribution());
      }

      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, allowed.toArray(new TestUser[0])))) {
         RestClusterClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get().cluster();
         assertStatus(FORBIDDEN, client.distribution());
      }
   }

   @Test
   public void testRestServerNodeReport() throws Exception {
      ext.assumeContainerMode();

      RestClusterClient restClient = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get().cluster();
      CompletionStage<RestResponse> distribution = restClient.distribution();
      RestResponse distributionResponse = CompletionStages.join(distribution);
      assertEquals(OK, distributionResponse.status());
      Json json = Json.read(distributionResponse.body());
      List<String> nodes = json.asJsonList().stream().map(j -> j.at("node_name").asString()).collect(Collectors.toList());

      RestServerClient client = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get().server();
      for (String name : nodes) {
         try (RestResponse response = CompletionStages.join(client.report(name))) {
            assertEquals(OK, response.status());
            assertEquals(MediaType.APPLICATION_GZIP_TYPE, response.header("content-type"));
            assertTrue(response.header("Content-Disposition").startsWith("attachment;"));
            assertValidTar(response.bodyAsStream());
         }
      }

      RestResponse response = CompletionStages.join(client.report("not-a-node-name"));
      assertEquals(NOT_FOUND, response.status());

      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN))) {
         RestServerClient otherClient = ext.rest().withClientConfiguration(restBuilders.get(user)).get().server();
         assertStatus(FORBIDDEN, otherClient.report(ext.getMethodName()));
      }
   }

   @Test
   public void testRestServerGeneralReport() throws Exception {
      ext.assumeContainerMode();

      RestServerClient client = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get().server();

      try (RestResponse response = CompletionStages.join(client.report())) {
         assertEquals(OK, response.status());
         assertEquals(MediaType.APPLICATION_GZIP_TYPE, response.header("content-type"));
         assertTrue(response.header("Content-Disposition").startsWith("attachment;"));
         assertValidTar(response.bodyAsStream());
      }

      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN))) {
         RestServerClient otherClient = ext.rest().withClientConfiguration(restBuilders.get(user)).get().server();
         assertStatus(FORBIDDEN, otherClient.report());
      }
   }

   private void assertValidTar(InputStream is) throws Exception {
      long contentSize = 0;
      try (GZIPInputStream gis = new GZIPInputStream(is);
           TarArchiveInputStream tar = new TarArchiveInputStream(new GzipCompressorInputStream(gis))) {
         ArchiveEntry entry;
         while ((entry = tar.getNextEntry()) != null) {
            if (entry.isDirectory()) continue;
            contentSize += entry.getSize();
         }
      }

      assertThat(contentSize).isPositive();
   }

   @Test
   public void testRestAdminsMustAccessBackupsAndRestores() {
      String BACKUP_NAME = "backup";
      RestContainerClient client = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get().container();
      assertStatus(ACCEPTED, client.createBackup(BACKUP_NAME));
      File zip = awaitStatus(() -> client.getBackup(BACKUP_NAME, false), ACCEPTED, OK, response -> {
         String fileName = response.header(CONTENT_DISPOSITION).split("=")[1];
         File backupZip = new File(ext.getServerDriver().getRootDir(), fileName);
         try (InputStream is = response.bodyAsStream()) {
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
         RestContainerClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get().container();
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
      RestClient adminClient = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get();
      WeakSSEListener sseListener = new WeakSSEListener();

      // admin must be able to listen events.
      try (Closeable ignored = adminClient.raw().listen("/rest/v2/container?action=listen", Collections.emptyMap(), sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));
      }

      // non-admins must receive 403 status code.
      for (TestUser nonAdmin : TestUser.NON_ADMINS) {
         CountDownLatch latch = new CountDownLatch(1);
         RestClient client = ext.rest().withClientConfiguration(restBuilders.get(nonAdmin)).get();
         WeakSSEListener listener = new WeakSSEListener() {
            @Override
            public void onError(Throwable t, RestResponseInfo response) {
               if (response.status() == FORBIDDEN) {
                  latch.countDown();
               }
            }
         };

         try (Closeable ignored = client.raw().listen("/rest/v2/container?action=listen", Collections.emptyMap(), listener)) {
            assertTrue(latch.await(10, TimeUnit.SECONDS));
         }
      }
   }

   @Test
   public void testConsoleLogin() {
      for (TestUser user : TestUser.ALL) {
         RestClientConfiguration cfg = restBuilders.get(user).build();
         boolean followRedirects = !cfg.security().authentication().mechanism().equals("SPNEGO");
         RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder().read(cfg, Combine.DEFAULT).clearServers().followRedirects(followRedirects);
         InetSocketAddress serverAddress = ext.getServerDriver().getServerSocket(0, 11222);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
         RestClient client = ext.rest().withClientConfiguration(builder).get();
         assertStatus(followRedirects ? OK : TEMPORARY_REDIRECT, client.raw().get("/rest/v2/login"));
         Json acl = Json.read(assertStatus(OK, client.raw().get("/rest/v2/security/user/acl")));
         Json subject = acl.asJsonMap().get("subject");
         Map<String, Object> principal = subject.asJsonList().get(0).asMap();
         assertEquals(serverPrincipal.apply(user), principal.get("name"));
      }
   }

   @Test
   public void testRestCacheNames() {
      restCreateAuthzCache("admin", "observer", "deployer");
      String name = ext.getMethodName();

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.OBSERVER, TestUser.DEPLOYER)) {
         try (RestResponse caches = CompletionStages.join(ext.rest().withClientConfiguration(restBuilders.get(type)).get().caches())) {
            assertEquals(OK, caches.status());
            Json json = Json.read(caches.body());
            Set<String> names = json.asJsonList().stream().map(Json::asString).collect(Collectors.toSet());
            assertTrue(names.contains(name), names.toString());
         }
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ADMIN, TestUser.OBSERVER, TestUser.DEPLOYER, TestUser.ANONYMOUS))) {
         try (RestResponse caches = CompletionStages.join(ext.rest().withClientConfiguration(restBuilders.get(type)).get().caches())) {
            assertEquals(OK, caches.status());
            Json json = Json.read(caches.body());
            Set<String> names = json.asJsonList().stream().map(Json::asString).collect(Collectors.toSet());
            assertFalse(names.contains(name), names.toString());
         }
      }
   }

   @Test
   public void testBulkReadUsersCanQuery() {
      createIndexedCache();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER, TestUser.APPLICATION, TestUser.OBSERVER)) {
         RestCacheClient userCache = ext.rest().withClientConfiguration(restBuilders.get(user)).get().cache(ext.getMethodName());
         assertStatus(OK, userCache.query("FROM sample_bank_account.User WHERE name = 'Tom'"));
         assertStatus(OK, userCache.searchStats());
         assertStatus(OK, userCache.indexStats());
         assertStatus(OK, userCache.queryStats());
      }
   }

   @Test
   public void testNonBulkReadUsersCannotQuery() {
      createIndexedCache();
      for (TestUser user : EnumSet.of(TestUser.READER, TestUser.WRITER, TestUser.MONITOR)) {
         RestCacheClient userCache = ext.rest().withClientConfiguration(restBuilders.get(user)).get().cache(ext.getMethodName());
         assertStatus(FORBIDDEN, userCache.query("FROM sample_bank_account.User WHERE name = 'Tom'"));
         assertStatus(OK, userCache.searchStats());
         assertStatus(OK, userCache.indexStats());
         assertStatus(OK, userCache.queryStats());
      }
   }

   @Test
   public void testFlushACL() {
      for (TestUser user : EnumSet.of(TestUser.ADMIN)) {
         RestClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get();
         assertStatus(NO_CONTENT, client.security().flushCache());
      }
      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ADMIN, TestUser.ANONYMOUS))) {
         RestClient client = ext.rest().withClientConfiguration(restBuilders.get(user)).get();
         assertStatus(FORBIDDEN, client.security().flushCache());
      }
   }

   @Test
   public void testRoleManipulation() {
      for (TestUser user : EnumSet.of(TestUser.ADMIN)) {
         RestSecurityClient security = ext.rest().withClientConfiguration(restBuilders.get(user)).get().security();
         assertStatus(OK, security.listUsers());
         assertStatus(NO_CONTENT, security.createRole("myrole", "my-role description", List.of("ALL_READ", "ALL_WRITE")));
         Json json = Json.read(assertStatus(OK, security.describeRole("myrole")));
         assertEquals("myrole", json.at("name").asString());
         assertEquals("my-role description", json.at("description").asString());
         assertFalse(json.at("implicit").asBoolean());
         List<Json> permissions = json.at("permissions").asJsonList();
         assertEquals(2, permissions.size());
         assertTrue(permissions.stream().map(Json::asString).collect(Collectors.toSet()).containsAll(List.of("ALL_READ", "ALL_WRITE")), permissions.toString());

         // The code below only works when using the cluster mapper
         try (RestResponse response = sync(security.grant("myuser", List.of("myrole")))) {
            if (response.status() == NO_CONTENT) {
               assertStatusAndBodyEquals(OK, "[\"myrole\"]", security.listRoles("myuser"));
               CompletionStage<RestResponse> listRoles = security.listRoles();
               ResponseAssertion.assertThat(listRoles).isOk();
               ResponseAssertion.assertThat(listRoles).hasReturnedText("[\"observer\",\"application\",\"reader\",\"admin\",\"monitor\",\"deployer\",\"writer\",\"myrole\"]");
               CompletionStage<RestResponse> listRolesDetailed = security.listRoles(true);
               ResponseAssertion.assertThat(listRolesDetailed).isOk();
               ResponseAssertion.assertThat(listRolesDetailed).containsInAnyOrderReturnedText("admin", "permissions", "inheritable", "implicit", "description");
               assertStatus(NO_CONTENT, security.deny("myuser", List.of("myrole")));
            }
         }
      }
      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ADMIN, TestUser.ANONYMOUS))) {
         RestSecurityClient security = ext.rest().withClientConfiguration(restBuilders.get(user)).get().security();
         assertStatus(FORBIDDEN, security.listUsers());
         assertStatus(FORBIDDEN, security.createRole("myrole", "description", List.of("ALL_READ", "ALL_WRITE")));
         assertStatus(FORBIDDEN, security.describeRole("myrole"));
         assertStatus(FORBIDDEN, security.grant("myuser", List.of("myrole")));
         assertStatus(FORBIDDEN, security.listRoles("myuser"));
         assertStatus(FORBIDDEN, security.deny("myuser", List.of("myrole")));
         assertStatus(FORBIDDEN, security.listRoles());
         assertStatus(FORBIDDEN, security.listRoles(true));
      }
   }

   @Test
   public void overviewReport() {
      RestClient restClient = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get();
      assertStatus(OK, restClient.server().overviewReport());

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ADMIN, TestUser.ANONYMOUS))) {
         restClient = ext.rest().withClientConfiguration(restBuilders.get(type)).get();
         assertStatus(FORBIDDEN, restClient.server().overviewReport());
      }
   }

   private void createIndexedCache() {
      String schema = TestDomainSCI.INSTANCE.getProtoFile();
      RestClient adminClient = ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).get();
      RestCacheClient protobufCache = adminClient.cache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      assertStatus(NO_CONTENT, protobufCache.put(BANK_PROTO, schema));

      String cacheName = ext.getMethodName();
      String cacheConfig = "{\"distributed-cache\":{\"statistics\":true,\"encoding\":{\"media-type\":\"application/x-protostream\"},\"indexing\":{\"enabled\":true,\"storage\":\"local-heap\",\"indexed-entities\":[\"sample_bank_account.User\"]},\"security\":{\"authorization\":{}}}}";

      assertStatus(OK,
            adminClient.cache(cacheName)
                  .createWithConfiguration(RestEntity.create(MediaType.APPLICATION_JSON, cacheConfig))
      );
   }

   private RestClient restCreateAuthzCache(String... explicitRoles) {
      return restCreateAuthzCache(CacheMode.DIST_SYNC, explicitRoles);
   }

   private RestClient restCreateAuthzLocalCache(String... explicitRoles) {
      return restCreateAuthzCache(CacheMode.LOCAL, explicitRoles);
   }

   private RestClient restCreateAuthzCache(CacheMode mode, String... explicitRoles) {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      AuthorizationConfigurationBuilder authorizationConfigurationBuilder = builder.clustering().cacheMode(mode).security().authorization().enable();
      if (explicitRoles != null) {
         for (String role : explicitRoles) {
            authorizationConfigurationBuilder.role(role);
         }
      }
      return ext.rest().withClientConfiguration(restBuilders.get(TestUser.ADMIN)).withServerConfiguration(builder).create();
   }
}
