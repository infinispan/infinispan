package org.infinispan.server.security;

import static org.infinispan.server.security.Common.sync;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.infinispan.server.test.core.LdapServerRule;
import org.infinispan.server.test.core.category.Security;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/

@Category(Security.class)
public class AuthorizationLDAPIT {
   @ClassRule
   public static InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/AuthorizationLDAPTest.xml")
               .build();

   @ClassRule
   public static LdapServerRule LDAP = new LdapServerRule(SERVERS);

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   final Map<String, ConfigurationBuilder> hotRodBuilders;
   final Map<String, RestClientConfigurationBuilder> restBuilders;

   final Map<String, String> bulkData;

   public AuthorizationLDAPIT() {
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

   private void addClientBuilders(String username, String password) {
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
      RemoteCache<String, String> adminCache = SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get("admin")).withCacheMode(CacheMode.DIST_SYNC).create();
      adminCache.put("k", "v");
      assertEquals("v", adminCache.get("k"));
      adminCache.putAll(bulkData);
      assertEquals(11, adminCache.size());
   }

   @Test
   public void testRestAdminCanDoEverything() {
      RestCacheClient adminCache = SERVER_TEST.rest().withClientConfiguration(restBuilders.get("admin")).withCacheMode(CacheMode.DIST_SYNC).create().cache(SERVER_TEST.getMethodName());
      sync(adminCache.put("k", "v"));
      assertEquals("v", sync(adminCache.get("k")).getBody());
   }

   @Test
   public void testHotRodNonAdminsMustNotCreateCache() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
               () -> SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
      }
   }

   @Test
   public void testRestNonAdminsMustNotCreateCache() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         Exceptions.expectException(RuntimeException.class, "(?s).*403.*",
               () -> SERVER_TEST.rest().withClientConfiguration(restBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
      }
   }

   @Test
   public void testHotRodWriterCannotRead() {
      hotRodCreateAuthzCache();
      RemoteCache<String, String> writerCache = SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get("writer")).get();
      writerCache.put("k1", "v1");
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> writerCache.get("k1")
      );
      for (String user : Arrays.asList("reader", "supervisor")) {
         RemoteCache<String, String> userCache = SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         assertEquals("v1", userCache.get("k1"));
      }
   }

   @Test
   public void testRestWriterCannotRead() {
      restCreateAuthzCache();
      RestCacheClient writerCache = SERVER_TEST.rest().withClientConfiguration(restBuilders.get("writer")).get().cache(SERVER_TEST.getMethodName());
      sync(writerCache.put("k1", "v1"));
      assertEquals(403, sync(writerCache.get("k1")).getStatus());
      for (String user : Arrays.asList("reader", "supervisor")) {
         RestCacheClient userCache = SERVER_TEST.rest().withClientConfiguration(restBuilders.get(user)).get().cache(SERVER_TEST.getMethodName());
         assertEquals("v1", sync(userCache.get("k1")).getBody());
      }
   }

   @Test
   public void testHotRodReaderCannotWrite() {
      hotRodCreateAuthzCache();
      RemoteCache<String, String> readerCache = SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get("reader")).get();
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> readerCache.put("k1", "v1")
      );
      for (String user : Arrays.asList("writer", "supervisor")) {
         RemoteCache<String, String> userCache = SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         userCache.put(user, user);
      }
   }

   @Test
   public void testRestReaderCannotWrite() {
      restCreateAuthzCache();
      RestCacheClient readerCache = SERVER_TEST.rest().withClientConfiguration(restBuilders.get("reader")).get().cache(SERVER_TEST.getMethodName());
      assertEquals(403, sync(readerCache.put("k1", "v1")).getStatus());
      for (String user : Arrays.asList("writer", "supervisor")) {
         RestCacheClient userCache = SERVER_TEST.rest().withClientConfiguration(restBuilders.get(user)).get().cache(SERVER_TEST.getMethodName());
         userCache.put(user, user);
      }
   }

   @Test
   public void testHotRodBulkOperations() {
      hotRodCreateAuthzCache().putAll(bulkData);
      RemoteCache<String, String> readerCache = SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get("reader")).get();
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> readerCache.getAll(bulkData.keySet())
      );
      RemoteCache<String, String> supervisorCache = SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get("supervisor")).get();
      supervisorCache.getAll(bulkData.keySet());
   }

   @Test
   public void testAnonymousHealthPredefinedCache() {
      RestClient client = SERVER_TEST.rest().get();
      assertEquals("HEALTHY", sync(client.cacheManager("default").healthStatus()).getBody());
   }

   private RemoteCache<Object, Object> hotRodCreateAuthzCache() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).security().authorization().enable().role("AdminRole").role("ReaderRole").role("WriterRole").role("SupervisorRole");
      return SERVER_TEST.hotrod().withClientConfiguration(hotRodBuilders.get("admin")).withServerConfiguration(builder).create();
   }

   private RestClient restCreateAuthzCache() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).security().authorization().enable().role("AdminRole").role("ReaderRole").role("WriterRole").role("SupervisorRole");
      return SERVER_TEST.rest().withClientConfiguration(restBuilders.get("admin")).withServerConfiguration(builder).create();
   }
}
