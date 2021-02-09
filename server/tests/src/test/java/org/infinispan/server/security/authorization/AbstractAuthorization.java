package org.infinispan.server.security.authorization;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/

public abstract class AbstractAuthorization {
   final Map<String, ConfigurationBuilder> hotRodBuilders;
   final Map<String, RestClientConfigurationBuilder> restBuilders;
   final Map<String, String> bulkData;

   protected AbstractAuthorization() {
      hotRodBuilders = new HashMap<>();
      restBuilders = new HashMap<>();
      addClientBuilders("admin", "strongPassword");
      addClientBuilders("writer", "somePassword");
      addClientBuilders("reader", "password");
      addClientBuilders("supervisor", "lessStrongPassword");
      bulkData = new HashMap<>();
      for (int i = 0; i < 10; i++) {
         bulkData.put("k" + i, "v" + i);
      }
   }

   protected abstract InfinispanServerTestMethodRule getServerTest();

   protected void addClientBuilders(String username, String password) {
      ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
      hotRodBuilder.security().authentication()
            .saslMechanism("SCRAM-SHA-1")
            .serverName("infinispan")
            .realm("default")
            .username(username)
            .password(password);
      hotRodBuilders.put(username, hotRodBuilder);
      RestClientConfigurationBuilder restBuilder = new RestClientConfigurationBuilder();
      restBuilder.security().authentication()
            .mechanism("AUTO")
            .username(username)
            .password(password);
      restBuilders.put(username, restBuilder);
   }

   @Test
   public void testHotRodAdminCanDoEverything() {
      RemoteCache<String, String> adminCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get("admin")).withCacheMode(CacheMode.DIST_SYNC).create();
      adminCache.put("k", "v");
      assertEquals("v", adminCache.get("k"));
      adminCache.putAll(bulkData);
      assertEquals(11, adminCache.size());
   }

   @Test
   public void testRestAdminCanDoEverything() {
      RestCacheClient adminCache = getServerTest().rest().withClientConfiguration(restBuilders.get("admin")).withCacheMode(CacheMode.DIST_SYNC).create().cache(getServerTest().getMethodName());
      sync(adminCache.put("k", "v"));
      assertEquals("v", sync(adminCache.get("k")).getBody());
   }

   @Test
   public void testHotRodNonAdminsMustNotCreateCache() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
               () -> getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
      }
   }

   @Test
   public void testRestNonAdminsMustNotCreateCache() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         Exceptions.expectException(SecurityException.class, "(?s).*403.*",
               () -> getServerTest().rest().withClientConfiguration(restBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
      }
   }

   @Test
   public void testHotRodWriterCannotReadImplicit() {
      testHotRodWriterCannotRead(false);
   }

   @Test
   public void testHotRodWriterCannotReadExplicit() {
      testHotRodWriterCannotRead(true);
   }

   private void testHotRodWriterCannotRead(boolean explicitRoles) {
      hotRodCreateAuthzCache(explicitRoles);
      RemoteCache<String, String> writerCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get("writer")).get();
      writerCache.put("k1", "v1");
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> writerCache.get("k1")
      );
      for (String user : Arrays.asList("reader", "supervisor")) {
         RemoteCache<String, String> userCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         assertEquals("v1", userCache.get("k1"));
      }
   }

   @Test
   public void testRestWriterCannotReadImplicit() {
      testRestWriterCannotRead(false);
   }

   @Test
   public void testRestWriterCannotReadExplicit() {
      testRestWriterCannotRead(true);
   }

   private void testRestWriterCannotRead(boolean explicitRoles) {
      restCreateAuthzCache(explicitRoles);
      RestCacheClient writerCache = getServerTest().rest().withClientConfiguration(restBuilders.get("writer")).get().cache(getServerTest().getMethodName());
      sync(writerCache.put("k1", "v1"));
      assertEquals(403, sync(writerCache.get("k1")).getStatus());
      for (String user : Arrays.asList("reader", "supervisor")) {
         RestCacheClient userCache = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(getServerTest().getMethodName());
         assertEquals("v1", sync(userCache.get("k1")).getBody());
      }
   }

   @Test
   public void testHotRodReaderCannotWriteImplicit() {
      testHotRodReaderCannotWrite(false);
   }

   @Test
   public void testHotRodReaderCannotWriteExplicit() {
      testHotRodReaderCannotWrite(true);
   }

   private void testHotRodReaderCannotWrite(boolean explicitRoles) {
      hotRodCreateAuthzCache(explicitRoles);
      RemoteCache<String, String> readerCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get("reader")).get();
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> readerCache.put("k1", "v1")
      );
      for (String user : Arrays.asList("writer", "supervisor")) {
         RemoteCache<String, String> userCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         userCache.put(user, user);
      }
   }

   @Test
   public void testRestReaderCannotWriteImplicit() {
      testRestReaderCannotWrite(false);
   }

   @Test
   public void testRestReaderCannotWriteExplicit() {
      testRestReaderCannotWrite(true);
   }

   private void testRestReaderCannotWrite(boolean explicitRoles) {
      restCreateAuthzCache(explicitRoles);
      RestCacheClient readerCache = getServerTest().rest().withClientConfiguration(restBuilders.get("reader")).get().cache(getServerTest().getMethodName());
      assertEquals(403, sync(readerCache.put("k1", "v1")).getStatus());
      for (String user : Arrays.asList("writer", "supervisor")) {
         RestCacheClient userCache = getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(getServerTest().getMethodName());
         userCache.put(user, user);
      }
   }

   @Test
   public void testHotRodBulkOperationsImplicit() {
      testHotRodBulkOperations(false);
   }

   @Test
   public void testHotRodBulkOperationsExplicit() {
      testHotRodBulkOperations(true);
   }

   private void testHotRodBulkOperations(boolean explicitRoles) {
      hotRodCreateAuthzCache(explicitRoles).putAll(bulkData);
      RemoteCache<String, String> readerCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get("reader")).get();
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

      RemoteCache<String, String> supervisorCache = getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get("supervisor")).get();
      supervisorCache.getAll(bulkData.keySet());
      //make sure iterator() is invoked (ISPN-12716)
      assertFalse(new HashSet<>(supervisorCache.keySet()).isEmpty());
      assertFalse(new HashSet<>(supervisorCache.values()).isEmpty());
      assertFalse(new HashSet<>(supervisorCache.entrySet()).isEmpty());
   }

   @Test
   public void testAnonymousHealthPredefinedCache() {
      RestClient client = getServerTest().rest().get();
      assertEquals("HEALTHY", sync(client.cacheManager("default").healthStatus()).getBody());
   }

   @Test
   public void testRestNonAdminsMustNotShutdownServer() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().stop()).getStatus());
      }
   }

   @Test
   public void testRestNonAdminsMustNotShutdownCluster() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().stop()).getStatus());
      }
   }

   @Test
   public void testRestNonAdminsMustNotModifyCacheIgnores() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().ignoreCache("default", "predefined")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().unIgnoreCache("default", "predefined")).getStatus());
      }
   }

   @Test
   public void testRestAdminsShouldBeAbleToModifyLoggers() {
      assertEquals(204, sync(getServerTest().rest().withClientConfiguration(restBuilders.get("admin")).get().server().logging().setLogger("org.infinispan.TEST_LOGGER", "ERROR", "STDOUT")).getStatus());
      assertEquals(204, sync(getServerTest().rest().withClientConfiguration(restBuilders.get("admin")).get().server().logging().removeLogger("org.infinispan.TEST_LOGGER")).getStatus());
   }

   @Test
   public void testRestNonAdminsMustNotModifyLoggers() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().logging().setLogger("org.infinispan.TEST_LOGGER", "ERROR", "STDOUT")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().logging().removeLogger("org.infinispan.TEST_LOGGER")).getStatus());
      }
   }

   @Test
   public void testRestNonAdminsMustNotObtainReport() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().server().report()).getStatus());
      }
   }

   @Test
   public void testRestNonAdminsMustNotAccessPerformXSiteOps() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").takeSiteOffline("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").bringSiteOnline("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").cancelPushState("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").cancelReceiveState("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").clearPushStateStatus()).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").pushSiteState("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").pushStateStatus()).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").xsiteBackups()).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").backupStatus("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").getXSiteTakeOfflineConfig("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache("xsite").updateXSiteTakeOfflineConfig("NYC", 10, 1000)).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cacheManager("default").bringBackupOnline("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cacheManager("default").takeOffline("NYC")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cacheManager("default").backupStatuses()).getStatus());
      }
   }

   @Test
   public void testRestNonAdminsMustNotPerformSearchActions() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      assertEquals(200, sync(getServerTest().rest().withClientConfiguration(restBuilders.get("admin")).get().schemas().put("bank.proto", schema)).getStatus());
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.indexing().enable().addIndexedEntity("sample_bank_account.User");
      getServerTest().rest().withClientConfiguration(restBuilders.get("admin")).withServerConfiguration(builder).create();
      String indexedCache = getServerTest().getMethodName();

      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(indexedCache).clearSearchStats()).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(indexedCache).reindex()).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cache(indexedCache).clearIndex()).getStatus());
      }
   }

   @Test
   public void testRestNonAdminsMustNotAccessBackupsAndRestores() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().createBackup("backup")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().getBackup("backup", true)).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().getBackupNames()).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().deleteBackup("backup")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().restore("restore", "somewhere")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().getRestoreNames()).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().getRestore("restore")).getStatus());
         assertEquals(403, sync(getServerTest().rest().withClientConfiguration(restBuilders.get(user)).get().cluster().deleteRestore("restore")).getStatus());
      }
   }

   private RemoteCache<Object, Object> hotRodCreateAuthzCache(boolean explicitRoles) {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      AuthorizationConfigurationBuilder authorizationConfigurationBuilder = builder.clustering().cacheMode(CacheMode.DIST_SYNC).security().authorization().enable();
      if (explicitRoles) {
         authorizationConfigurationBuilder.role("AdminRole").role("ReaderRole").role("WriterRole").role("SupervisorRole");
      }
      return getServerTest().hotrod().withClientConfiguration(hotRodBuilders.get("admin")).withServerConfiguration(builder).create();
   }

   private RestClient restCreateAuthzCache(boolean explicitRoles) {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      AuthorizationConfigurationBuilder authorizationConfigurationBuilder = builder.clustering().cacheMode(CacheMode.DIST_SYNC).security().authorization().enable();
      if (explicitRoles) {
         authorizationConfigurationBuilder.role("AdminRole").role("ReaderRole").role("WriterRole").role("SupervisorRole");
      }
      return getServerTest().rest().withClientConfiguration(restBuilders.get("admin")).withServerConfiguration(builder).create();
   }
}
