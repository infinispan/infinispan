package org.infinispan.server.security.authorization;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.test.core.TestSystemPropertyNames.HOTROD_CLIENT_SASL_MECHANISM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.skip.SkipJunit;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.jboss.marshalling.commons.GenericJBossMarshaller;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.functional.hotrod.HotRodCacheQueries;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;

abstract class HotRodAuthorizationTest {

   public static final String BANK_PROTO = "bank.proto";
   public static final String UNAUTHORIZED_EXCEPTION = "(?s).*ISPN000287.*";

   protected final Map<TestUser, ConfigurationBuilder> hotRodBuilders;
   protected final InfinispanServerExtension ext;
   protected final Function<TestUser, String> serverPrincipal;

   protected final Map<String, String> bulkData;

   public HotRodAuthorizationTest(InfinispanServerExtension ext) {
      this (ext, TestUser::getUser, user -> {
         ConfigurationBuilder hotRodBuilder = new ConfigurationBuilder();
         hotRodBuilder.security().authentication()
               .saslMechanism(System.getProperty(HOTROD_CLIENT_SASL_MECHANISM, "SCRAM-SHA-1"))
               .serverName("infinispan")
               .realm("default")
               .username(user.getUser())
               .password(user.getPassword());
         return hotRodBuilder;
      });
   }

   public HotRodAuthorizationTest(InfinispanServerExtension ext, Function<TestUser, String> serverPrincipal, Function<TestUser, ConfigurationBuilder> hotRodBuilder) {
      this.ext = ext;
      this.serverPrincipal = serverPrincipal;

      this.hotRodBuilders = Stream.of(TestUser.values()).collect(Collectors.toMap(user -> user, hotRodBuilder));

      this.bulkData = IntStream.range(0, 10)
            .boxed()
            .collect(Collectors.toMap(i -> "k"+i, i -> "v"+i));
   }

   @Test
   public void testHotRodAdminAndDeployerCanDoEverything() {
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCache<String, String> cache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create();
         cache.put("k", "v");
         assertEquals("v", cache.get("k"));
         cache.putAll(bulkData);
         assertEquals(11, cache.size());
         cache.getRemoteCacheManager().administration().removeCache(cache.getName());
      }
   }

   @Test
   public void testHotRodNonAdminsMustNotCreateCache() {
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.MONITOR)) {
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).withCacheMode(CacheMode.DIST_SYNC).create()
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
      hotRodCreateAuthzCache(explicitRoles).putAll(bulkData);
      RemoteCache<String, String> readerCache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.READER)).get();
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> readerCache.getAll(bulkData.keySet())
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

      RemoteCache<String, String> supervisorCache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.DEPLOYER)).get();
      supervisorCache.getAll(bulkData.keySet());
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
      RemoteCache<String, String> readerCache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.OBSERVER)).get();
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> readerCache.put("k1", "v1")
      );
      for (TestUser user : EnumSet.of(TestUser.DEPLOYER, TestUser.APPLICATION, TestUser.WRITER)) {
         RemoteCache<String, String> userCache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         userCache.put(user.name(), user.name());
      }
   }

   @Test
   public void testScriptUpload() {
      SkipJunit.skipCondition(() -> ext.getServerDriver().getConfiguration().runMode() != ServerRunMode.CONTAINER);
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCacheManager remoteCacheManager = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).createRemoteCacheManager();
         ext.addScript(remoteCacheManager, "scripts/test.js");
      }

      for (TestUser user : EnumSet.of(TestUser.MONITOR, TestUser.APPLICATION, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCacheManager remoteCacheManager = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).createRemoteCacheManager();
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> ext.addScript(remoteCacheManager, "scripts/test.js")
         );
      }
   }

   @Test
   public void testAdminCanRemoveCacheWithoutRole() {
      RemoteCache<String, String> adminCache = hotRodCreateAuthzCache("application");
      RemoteCache<String, String> appCache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.APPLICATION)).get();
      appCache.put("k1", "v1");
      adminCache.getRemoteCacheManager().administration().removeCache(adminCache.getName());
   }

   @Test
   public void testExecScripts() {
      SkipJunit.skipCondition(() -> ext.getServerDriver().getConfiguration().runMode() != ServerRunMode.CONTAINER);
      RemoteCache cache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).create();
      String scriptName = ext.addScript(cache.getRemoteCacheManager(), "scripts/test.js");
      Map params = new HashMap<>();
      params.put("key", "k");
      params.put("value", "v");

      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.APPLICATION, TestUser.DEPLOYER)) {
         RemoteCache cacheExec = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         cacheExec.execute(scriptName, params);
      }

      for (TestUser user : EnumSet.of(TestUser.MONITOR, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCache cacheExec = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> {
                  cacheExec.execute(scriptName, params);
               }
         );
      }
   }

   @Test
   public void testServerTaskWithParameters() {
      ext.assumeContainerMode();

      ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).create();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.APPLICATION, TestUser.DEPLOYER)) {
         RemoteCache<String, String> cache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         ArrayList<String> messages = cache.execute("hello", Collections.singletonMap("greetee", new ArrayList<>(Arrays.asList("nurse", "kitty"))));
         assertEquals(2, messages.size());
         assertEquals("Hello nurse", messages.get(0));
         assertEquals("Hello kitty", messages.get(1));
         String message = cache.execute("hello", Collections.emptyMap());
         assertEquals("Hello " + serverPrincipal.apply(user), message);
      }

      for (TestUser user : EnumSet.of(TestUser.MONITOR, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCache<String, String> cache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> cache.execute("hello", Collections.singletonMap("greetee", new ArrayList<>(Arrays.asList("nurse", "kitty"))))
         );
      }
   }

   @Test
   public void testCacheUpdateConfigurationAttribute() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder.memory().maxCount(100);
      ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).withServerConfiguration(builder).create();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCache<String, String> cache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         cache.getRemoteCacheManager().administration().updateConfigurationAttribute(cache.getName(), "memory.max-count", "1000");
      }

      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER, TestUser.ANONYMOUS))) {
         RemoteCache<String, String> cache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
               () -> cache.getRemoteCacheManager().administration().updateConfigurationAttribute(cache.getName(), "memory.max-count", "500")
         );
      }
   }

   @Test
   public void testDistributedServerTaskWithParameters() {
      ext.assumeContainerMode();

      ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).create();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.APPLICATION, TestUser.DEPLOYER)) {
         // We must utilise the GenericJBossMarshaller due to ISPN-8814
         RemoteCache<String, String> cache = ext.hotrod().withMarshaller(GenericJBossMarshaller.class).withClientConfiguration(hotRodBuilders.get(user)).get();
         List<String> greetings = cache.execute("dist-hello", Collections.singletonMap("greetee", "my friend"));
         assertEquals(2, greetings.size());
         for (String greeting : greetings) {
            assertTrue(greeting.matches("Hello my friend .*"));
         }
         greetings = cache.execute("dist-hello", Collections.emptyMap());
         assertEquals(2, greetings.size());
         for (String greeting : greetings) {
            assertTrue(greeting.startsWith("Hello " + serverPrincipal.apply(user) + " from "), greeting);
         }
      }
   }

   @Test
   public void testAdminAndDeployerCanManageSchema() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER)) {
         RemoteCacheManager remoteCacheManager = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).createRemoteCacheManager();
         RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
         metadataCache.put(BANK_PROTO, schema);
         metadataCache.remove(BANK_PROTO);
      }
   }

   @Test
   public void testNonCreatorsSchema() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      for (TestUser user : EnumSet.of(TestUser.APPLICATION, TestUser.OBSERVER, TestUser.WRITER)) {
         RemoteCacheManager remoteCacheManager = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).createRemoteCacheManager();
         RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION, () -> metadataCache.put(BANK_PROTO, schema));
      }
   }

   @Test
   public void testBulkReadUsersCanQuery() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = prepareIndexedCache();
      for (TestUser user : EnumSet.of(TestUser.ADMIN, TestUser.DEPLOYER, TestUser.APPLICATION, TestUser.OBSERVER)) {
         RemoteCache<Integer, User> userCache = ext.hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(user)).withServerConfiguration(builder).get();
         User fromCache = userCache.get(1);
         HotRodCacheQueries.assertUser1(fromCache);
         Query<User> query = userCache.query("FROM sample_bank_account.User WHERE name = 'Tom'");
         List<User> list = query.execute().list();
         assertNotNull(list);
         assertEquals(1, list.size());
         assertEquals(User.class, list.get(0).getClass());
         HotRodCacheQueries.assertUser1(list.get(0));
      }
   }

   @Test
   public void testNonBulkReadUsersCannotQuery() {
      org.infinispan.configuration.cache.ConfigurationBuilder builder = prepareIndexedCache();
      for (TestUser user : EnumSet.of(TestUser.READER, TestUser.WRITER)) {
         RemoteCache<Integer, User> userCache = ext.hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(user)).withServerConfiguration(builder).get();
         Query<User> query = userCache.query("FROM sample_bank_account.User WHERE name = 'Tom'");
         Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION, () -> query.execute().list());
      }
   }

   @Test
   public void testHotRodCacheNames() {
      hotRodCreateAuthzCache("admin", "observer", "deployer");
      String name = ext.getMethodName();

      for (TestUser type : EnumSet.of(TestUser.ADMIN, TestUser.OBSERVER, TestUser.DEPLOYER)) {
         Set<String> caches = ext.hotrod().withClientConfiguration(hotRodBuilders.get(type)).get().getRemoteCacheContainer().getCacheNames();
         assertTrue(caches.contains(name), caches.toString());
      }

      // Types with no access.
      EnumSet<TestUser> types = EnumSet.complementOf(EnumSet.of(TestUser.ADMIN, TestUser.OBSERVER, TestUser.DEPLOYER, TestUser.ANONYMOUS));
      // APPLICATION, MONITOR, READER, WRITER
      System.out.println(types);
      for (TestUser type : types) {
         Set<String> caches = ext.hotrod().withClientConfiguration(hotRodBuilders.get(type)).get().getRemoteCacheContainer().getCacheNames();
         assertFalse(caches.contains(name), caches.toString());
      }
   }

   private org.infinispan.configuration.cache.ConfigurationBuilder prepareIndexedCache() {
      String schema = Exceptions.unchecked(() -> Util.getResourceAsString("/sample_bank_account/bank.proto", this.getClass().getClassLoader()));
      RemoteCacheManager remoteCacheManager = ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).createRemoteCacheManager();
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put(BANK_PROTO, schema);

      org.infinispan.configuration.cache.ConfigurationBuilder builder = new org.infinispan.configuration.cache.ConfigurationBuilder();
      builder
            .clustering().cacheMode(CacheMode.DIST_SYNC).stateTransfer().awaitInitialTransfer(true)
            .security().authorization().enable()
            .indexing().enable().storage(LOCAL_HEAP).addIndexedEntity("sample_bank_account.User");

      RemoteCache<Integer, User> adminCache = ext.hotrod().withClientConfiguration(clientConfigurationWithProtostreamMarshaller(TestUser.ADMIN)).withServerConfiguration(builder).create();
      adminCache.put(1, HotRodCacheQueries.createUser1());
      adminCache.put(2, HotRodCacheQueries.createUser2());
      return builder;
   }

   private ConfigurationBuilder clientConfigurationWithProtostreamMarshaller(TestUser user) {
      ConfigurationBuilder client = new ConfigurationBuilder().read(hotRodBuilders.get(user).build(), Combine.DEFAULT);
      client.servers().clear();
      ProtoStreamMarshaller marshaller = new ProtoStreamMarshaller();
      client.marshaller(marshaller);
      Exceptions.unchecked(() -> MarshallerRegistration.registerMarshallers(marshaller.getSerializationContext()));
      return client;
   }

   private void testHotRodWriterCannotRead(String... explicitRoles) {
      hotRodCreateAuthzCache(explicitRoles);
      RemoteCache<String, String> writerCache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.WRITER)).get();
      writerCache.put("k1", "v1");
      Exceptions.expectException(HotRodClientException.class, UNAUTHORIZED_EXCEPTION,
            () -> writerCache.get("k1")
      );
      for (TestUser user : EnumSet.complementOf(EnumSet.of(TestUser.WRITER, TestUser.MONITOR, TestUser.ANONYMOUS))) {
         RemoteCache<String, String> userCache = ext.hotrod().withClientConfiguration(hotRodBuilders.get(user)).get();
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
      return ext.hotrod().withClientConfiguration(hotRodBuilders.get(TestUser.ADMIN)).withServerConfiguration(builder).create();
   }
}
