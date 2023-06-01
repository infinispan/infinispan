package org.infinispan.server.security.authorization;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestCacheManagerClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestClusterClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.RestServerClient;
import org.infinispan.client.rest.XSiteStateTransferMode;
import org.infinispan.client.rest.configuration.RestClientConfiguration;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.rest.resources.WeakSSEListener;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ContainerInfinispanServerDriver;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.util.concurrent.CompletionStages;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Security.class)
public class RESTAuthorizationTest extends BaseTest {

   @Test
   public void testRestAdminCanDoEverything() {
      RestCacheClient adminCache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN))
            .withCacheMode(CacheMode.DIST_SYNC).create().cache(suite.getServerTest().getMethodName());
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
         RestCacheClient cache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(type)).get()
               .cache(suite.getServerTest().getMethodName());
         assertStatus(OK, cache.distribution());
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN, TestUser.MONITOR))) {
         RestCacheClient cache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(type)).get().cache(suite.getServerTest().getMethodName());
         assertStatus(FORBIDDEN, cache.distribution());
      }
   }

   @Test
   public void testRestKeyDistribution() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");
      RestCacheClient adminCache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN))
            .get().cache(suite.getServerTest().getMethodName());
      assertStatus(NO_CONTENT, adminCache.put("existentKey", "v"));

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.MONITOR)) {
         RestCacheClient cache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(type)).get()
               .cache(suite.getServerTest().getMethodName());
         assertStatus(OK, cache.distribution("somekey"));
         assertStatus(OK, cache.distribution("existentKey"));
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN, TestUser.MONITOR))) {
         RestCacheClient cache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(type)).get()
               .cache(suite.getServerTest().getMethodName());
         assertStatus(FORBIDDEN, cache.distribution("somekey"));
         assertStatus(FORBIDDEN, cache.distribution("existentKey"));
      }
   }

   @Test
   public void testStats() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.MONITOR)) {
         RestCacheClient cache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(type)).get()
               .cache(suite.getServerTest().getMethodName());
         assertStatus(OK, cache.stats());
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN, TestUser.MONITOR))) {
         RestCacheClient cache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(type)).get()
               .cache(suite.getServerTest().getMethodName());
         assertStatus(FORBIDDEN, cache.stats());
      }
   }

   @Test
   public void testStatsReset() {
      restCreateAuthzCache("admin", "observer", "deployer", "application", "writer", "reader", "monitor");

      for (TestUser type : EnumSet.of(TestUser.ADMIN)) {
         RestCacheClient cache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(type)).get()
               .cache(suite.getServerTest().getMethodName());
         assertStatus(NO_CONTENT, cache.statsReset());
      }

      // Types with no access.
      for (TestUser type : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN))) {
         RestCacheClient cache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(type)).get()
               .cache(suite.getServerTest().getMethodName());
         assertStatus(FORBIDDEN, cache.statsReset());
      }
   }

   @Test
   public void testRestNonAdminsMustNotCreateCache() {
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.MONITOR)) {
         Exceptions.expectException(SecurityException.class, "(?s).*403.*",
               () -> suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
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
      RestCacheClient writerCache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.WRITER)).get()
            .cache(suite.getServerTest().getMethodName());
      assertStatus(NO_CONTENT, writerCache.put("k1", "v1"));
      assertStatus(FORBIDDEN, writerCache.get("k1"));
      for (TestUser user : EnumSet.of(TestUser.OBSERVER, TestUser.DEPLOYER)) {
         RestCacheClient userCache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get()
               .cache(suite.getServerTest().getMethodName());
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
      RestCacheClient readerCache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.OBSERVER)).get()
            .cache(suite.getServerTest().getMethodName());
      assertStatus(FORBIDDEN, readerCache.put("k1", "v1"));
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.DEPLOYER)) {
         RestCacheClient userCache = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get()
               .cache(suite.getServerTest().getMethodName());
         assertStatus(NO_CONTENT, userCache.put(user.name(), user.name()));
      }
   }

   @Test
   public void testAnonymousHealthPredefinedCache() {
      RestClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ANONYMOUS)).get();
      assertStatusAndBodyEquals(OK, "HEALTHY", client.cacheManager("default").healthStatus());
   }

   @Test
   public void testRestNonAdminsMustNotShutdownServer() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().server().stop());
      }
   }

   @Test
   public void testRestNonAdminsMustNotShutdownCluster() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().cluster().stop());
      }
   }

   @Test
   public void testRestNonAdminsMustNotModifyCacheIgnores() {
      for (TestUser user : TestUser.NON_ADMINS) {
         RestClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get();
         assertStatus(FORBIDDEN, client.server().ignoreCache("default", "predefined"));
         assertStatus(FORBIDDEN, client.server().unIgnoreCache("default", "predefined"));
      }
   }

   @Test
   public void testRestAdminsShouldBeAbleToModifyLoggers() {
      RestClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN)).get();
      assertStatus(NO_CONTENT, client.server().logging().setLogger("org.infinispan.TEST_LOGGER", "ERROR", "STDOUT"));
      assertStatus(NO_CONTENT, client.server().logging().removeLogger("org.infinispan.TEST_LOGGER"));
   }

   @Test
   public void testRestNonAdminsMustNotModifyLoggers() {
      for (TestUser user : TestUser.NON_ADMINS) {
         assertStatus(FORBIDDEN, suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().server().logging().setLogger("org.infinispan.TEST_LOGGER", "ERROR", "STDOUT"));
         assertStatus(FORBIDDEN, suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().server().logging().removeLogger("org.infinispan.TEST_LOGGER"));
      }
   }

   @Test
   public void testRestAdminsShouldBeAbleToAdminServer() {
      RestClientConfigurationBuilder adminConfig = suite.restBuilders.get(TestUser.ADMIN);
      RestClient client = suite.getServerTest().rest().withClientConfiguration(adminConfig).get();
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
         RestClientConfigurationBuilder userConfig = suite.restBuilders.get(user);
         RestClient client = suite.getServerTest().rest().withClientConfiguration(userConfig).get();
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
      RestClientConfigurationBuilder userConfig = suite.restBuilders.get(user);
      RestClient client = suite.getServerTest().rest().withClientConfiguration(userConfig).get();
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
      assertStatus(OK, suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN)).get().schemas().put(BANK_PROTO, schema));
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.indexing().enable().addIndexedEntity("sample_bank_account.User").statistics().enable();
      RestClient restClient = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN))
            .withServerConfiguration(builder).create();
      String indexedCache = suite.getServerTest().getMethodName();
      RestCacheClient cache = restClient.cache(indexedCache);
      for (TestUser user : TestUser.NON_ADMINS) {
         searchActions(user, indexedCache, FORBIDDEN, FORBIDDEN);
      }
      searchActions(TestUser.ADMIN, indexedCache, OK, NO_CONTENT);
   }

   private void searchActions(TestUser user, String indexedCache, int status, int noContentStatus) {
      RestClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get();
      assertStatus(status, client.cache(indexedCache).clearSearchStats());
      assertStatus(noContentStatus, client.cache(indexedCache).reindex());
      assertStatus(noContentStatus, client.cache(indexedCache).clearIndex());
   }

   @Test
   public void testRestClusterDistributionPermission() {
      EnumSet<TestUser> allowed = EnumSet.of(TestUser.ADMIN, TestUser.MONITOR);
      for (TestUser user : allowed) {
         RestClusterClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().cluster();
         assertStatus(OK, client.distribution());
      }

      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, allowed.toArray(new TestUser[0])))) {
         RestClusterClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().cluster();
         assertStatus(FORBIDDEN, client.distribution());
      }
   }

   @Test
   public void testRestServerNodeReport() {
      if (!(suite.getServers().getServerDriver() instanceof ContainerInfinispanServerDriver)) {
         throw new AssumptionViolatedException("Requires CONTAINER mode");
      }
      RestClusterClient restClient = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN)).get().cluster();
      CompletionStage<RestResponse> distribution = restClient.distribution();
      RestResponse distributionResponse = CompletionStages.join(distribution);
      assertEquals(OK, distributionResponse.getStatus());
      Json json = Json.read(distributionResponse.getBody());
      List<String> nodes = json.asJsonList().stream().map(j -> j.at("node_name").asString()).collect(Collectors.toList());

      RestServerClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN)).get().server();
      for (String name : nodes) {
         RestResponse response = CompletionStages.join(client.report(name));
         assertEquals(OK, response.getStatus());
         assertEquals("application/gzip", response.getHeader("content-type"));
         assertTrue(response.getHeader("Content-Disposition").startsWith("attachment;"));
      }

      RestResponse response = CompletionStages.join(client.report("not-a-node-name"));
      assertEquals(NOT_FOUND, response.getStatus());

      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ANONYMOUS, TestUser.ADMIN))) {
         RestServerClient otherClient = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().server();
         assertStatus(FORBIDDEN, otherClient.report(suite.getServerTest().getMethodName()));
      }
   }

   @Test
   public void testRestAdminsMustAccessBackupsAndRestores() {
      String BACKUP_NAME = "backup";
      RestClusterClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN)).get().cluster();
      assertStatus(ACCEPTED, client.createBackup(BACKUP_NAME));
      File zip = awaitStatus(() -> client.getBackup(BACKUP_NAME, false), ACCEPTED, OK, response -> {
         String fileName = response.getHeader("Content-Disposition").split("=")[1];
         File backupZip = new File(suite.getServers().getServerDriver().getRootDir(), fileName);
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
         RestClusterClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(user)).get().cluster();
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
      RestClient adminClient = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN)).get();
      WeakSSEListener sseListener = new WeakSSEListener();

      // admin must be able to listen events.
      try (Closeable ignored = adminClient.raw().listen("/rest/v2/container?action=listen", Collections.emptyMap(), sseListener)) {
         assertTrue(sseListener.await(10, TimeUnit.SECONDS));
      }

      // non-admins must receive 403 status code.
      for (TestUser nonAdmin : TestUser.NON_ADMINS) {
         CountDownLatch latch = new CountDownLatch(1);
         RestClient client = suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(nonAdmin)).get();
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

   @Test
   public void testConsoleLogin() {
      for (TestUser user : TestUser.ALL) {
         RestClientConfiguration cfg = suite.restBuilders.get(user).build();
         boolean followRedirects = !cfg.security().authentication().mechanism().equals("SPNEGO");
         RestClientConfigurationBuilder builder = new RestClientConfigurationBuilder().read(cfg).clearServers().followRedirects(followRedirects);
         InetSocketAddress serverAddress = suite.getServers().getServerDriver().getServerSocket(0, 11222);
         builder.addServer().host(serverAddress.getHostName()).port(serverAddress.getPort());
         RestClient client = suite.getServerTest().rest().withClientConfiguration(builder).get();
         assertStatus(followRedirects ? OK : TEMPORARY_REDIRECT, client.raw().get("/rest/v2/login"));
         Json acl = Json.read(assertStatus(OK, client.raw().get("/rest/v2/security/user/acl")));
         Json subject = acl.asJsonMap().get("subject");
         Map<String, Object> principal = subject.asJsonList().get(0).asMap();
         assertEquals(expectedServerPrincipalName(user), principal.get("name"));
      }
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
      return suite.getServerTest().rest().withClientConfiguration(suite.restBuilders.get(TestUser.ADMIN)).withServerConfiguration(builder).create();
   }

   protected String expectedServerPrincipalName(TestUser user) {
      return suite.expectedServerPrincipalName(user);
   }
}
