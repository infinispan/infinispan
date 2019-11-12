package org.infinispan.server.security;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.test.InfinispanServerRule;
import org.infinispan.server.test.InfinispanServerRuleConfigurationBuilder;
import org.infinispan.server.test.InfinispanServerTestMethodRule;
import org.infinispan.server.test.LdapServerRule;
import org.infinispan.server.test.category.Security;
import org.infinispan.test.Exceptions;
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
   public static InfinispanServerRule SERVERS = new InfinispanServerRule(new InfinispanServerRuleConfigurationBuilder("configuration/AuthorizationLDAPTest.xml"));

   @ClassRule
   public static LdapServerRule LDAP = new LdapServerRule(SERVERS);

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   final Map<String, ConfigurationBuilder> builderMap;

   final Map<String, String> bulkData;

   public AuthorizationLDAPIT() {
      builderMap = new HashMap<>();
      addBuilder("admin", "strongPassword");
      addBuilder("writer", "somePassword");
      addBuilder("reader", "password");
      addBuilder("supervisor", "lessStrongPassword");
      bulkData = new HashMap<>();
      for (int i = 0; i < 10; i++) {
         bulkData.put("k" + i, "v" + i);
      }
   }

   private void addBuilder(String username, String password) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.security().authentication()
            .saslMechanism("SCRAM-SHA-1")
            .serverName("infinispan")
            .realm("default")
            .username(username)
            .password(password);
      builderMap.put(username, builder);
   }

   @Test
   public void testAdminCanDoEverything() {
      RemoteCache<String, String> adminCache = SERVER_TEST.hotrod().withClientConfiguration(builderMap.get("admin")).withCacheMode(CacheMode.DIST_SYNC).create();
      adminCache.put("k", "v");
      assertEquals("v", adminCache.get("k"));
      adminCache.putAll(bulkData);
      assertEquals(11, adminCache.size());
   }

   @Test
   public void testNonAdminsMustNotCreateCache() {
      for (String user : Arrays.asList("reader", "writer", "supervisor")) {
         Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
               () -> SERVER_TEST.hotrod().withClientConfiguration(builderMap.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
         );
      }
   }

   @Test
   public void testWriterCannotRead() {
      createAuthzCache();
      RemoteCache<String, String> writerCache = SERVER_TEST.hotrod().withClientConfiguration(builderMap.get("writer")).get();
      writerCache.put("k1", "v1");
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> writerCache.get("k1")
      );
      for (String user : Arrays.asList("reader", "supervisor")) {
         RemoteCache<String, String> userCache = SERVER_TEST.hotrod().withClientConfiguration(builderMap.get(user)).get();
         assertEquals("v1", userCache.get("k1"));
      }
   }

   @Test
   public void testReaderCannotWrite() {
      createAuthzCache();
      RemoteCache<String, String> readerCache = SERVER_TEST.hotrod().withClientConfiguration(builderMap.get("reader")).get();
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> readerCache.put("k1", "v1")
      );
      for (String user : Arrays.asList("writer", "supervisor")) {
         RemoteCache<String, String> userCache = SERVER_TEST.hotrod().withClientConfiguration(builderMap.get(user)).get();
         userCache.put(user, user);
      }
   }

   @Test
   public void testBulkOperations() {
      createAuthzCache().putAll(bulkData);
      RemoteCache<String, String> readerCache = SERVER_TEST.hotrod().withClientConfiguration(builderMap.get("reader")).get();
      Exceptions.expectException(HotRodClientException.class, "(?s).*ISPN000287.*",
            () -> readerCache.getAll(bulkData.keySet())
      );
      RemoteCache<String, String> supervisorCache = SERVER_TEST.hotrod().withClientConfiguration(builderMap.get("supervisor")).get();
      supervisorCache.getAll(bulkData.keySet());
   }

   private RemoteCache<Object, Object> createAuthzCache() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).security().authorization().enable().role("AdminRole").role("ReaderRole").role("WriterRole").role("SupervisorRole");
      return SERVER_TEST.hotrod().withClientConfiguration(builderMap.get("admin")).withServerConfiguration(builder).create();
   }
}
