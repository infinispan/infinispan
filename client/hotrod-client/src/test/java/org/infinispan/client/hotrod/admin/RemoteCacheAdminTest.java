package org.infinispan.client.hotrod.admin;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.infinispan.client.hotrod.DefaultTemplate;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManagerAdmin;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.query.testdomain.protobuf.TransactionPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.configuration.BasicConfiguration;
import org.infinispan.commons.configuration.StringConfiguration;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.RemoteCacheAdminTest")
public class RemoteCacheAdminTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      builder.indexing().enable()
             .storage(LOCAL_HEAP)
             .addIndexedEntity("sample_bank_account.Transaction");
      createHotRodServers(2, builder);
   }

   @Override
   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.defaultCacheName("default");
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      gcb.serialization().addContextInitializer(contextInitializer());

      EmbeddedCacheManager cm = addClusterEnabledCacheManager(gcb, builder);
      cm.defineConfiguration("template", builder.build());
      cm.defineConfiguration(DefaultTemplate.DIST_ASYNC.getTemplateName(), builder.build());
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder);
      servers.add(server);
      return server;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return TestDomainSCI.INSTANCE;
   }

   public void templateCreateRemoveTest(Method m) {
      String templateName = m.getName();
      String xml = String.format(
            "<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>",
            templateName);
      BasicConfiguration template = new StringConfiguration(xml);
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
               .createTemplate(templateName, template);
      assertTrue(manager(0).getCacheConfigurationNames().contains(templateName));
      assertTrue(manager(0).getCacheConfigurationNames().contains(templateName));
      client(1).administration().removeTemplate(templateName);
      assertFalse(manager(0).getCacheConfigurationNames().contains(templateName));
      assertFalse(manager(0).getCacheConfigurationNames().contains(templateName));
   }

   public void cacheCreateRemoveTest(Method m) {
      String cacheName = m.getName();
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, "template");
      assertTrue(manager(0).cacheExists(cacheName));
      assertTrue(manager(1).cacheExists(cacheName));
      client(1).administration().removeCache(cacheName);
      assertFalse(manager(0).cacheExists(cacheName));
      assertFalse(manager(1).cacheExists(cacheName));
   }

   public void cacheCreateMissingTemplate(Method m) throws InterruptedException {
      RemoteCacheManagerAdmin admin = client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE);

      // Creation with a non-existent template fails
      expectException(HotRodClientException.class,
                      () -> admin.createCache("some_name", "some_template_name"));

      // Cache can still be created later with a string configuration
      admin.createCache("some_name",
                        new StringConfiguration("<infinispan><cache-container>" +
                                                   "<local-cache name=\"some_name\"/>" +
                                                   "</cache-container></infinispan>"));
      RemoteCache<Object, Object> someCache = client(0).getCache("some_name");
      someCache.put("key", "value");
   }

   public void cacheCreateRemoveTestWithDefaultTemplateEnum(Method m) {
      String cacheName = m.getName();
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
               .createCache(cacheName, DefaultTemplate.DIST_ASYNC);
      assertTrue(manager(0).cacheExists(cacheName));
      assertTrue(manager(1).cacheExists(cacheName));
      client(1).administration().removeCache(cacheName);
      assertFalse(manager(0).cacheExists(cacheName));
      assertFalse(manager(1).cacheExists(cacheName));
   }

   public void cacheGetOrCreateRemoveTestWithDefaultTemplateEnum(Method m) {
      String cacheName = m.getName();
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
               .getOrCreateCache(cacheName, DefaultTemplate.DIST_ASYNC);
      assertTrue(manager(0).cacheExists(cacheName));
      assertTrue(manager(1).cacheExists(cacheName));
      client(1).administration().removeCache(cacheName);
      assertFalse(manager(0).cacheExists(cacheName));
      assertFalse(manager(1).cacheExists(cacheName));
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN000374.*")
   public void nonExistentTemplateTest(Method m) {
      String cacheName = m.getName();
      client(0).administration().createCache(cacheName, "nonExistentTemplate");
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*ISPN000507.*")
   public void alreadyExistingCacheTest(Method m) {
      String cacheName = m.getName();
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, "template");
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, "template");
   }

   public void getOrCreateWithTemplateTest(Method m) {
      String cacheName = m.getName();
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, "template");
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
               .getOrCreateCache(cacheName, "template");
   }

   public void getOrCreateWithoutTemplateTest() {
      client(0).administration().getOrCreateCache("default", (String) null);
   }

   public void cacheCreateWithXMLConfigurationTest(Method m) {
      String cacheName = m.getName();
      String xml = String.format(
            "<infinispan><cache-container><distributed-cache name=\"%s\"><encoding><key " +
            "media-type=\"text/plain\"/><value media-type=\"application/json\"/></encoding><expiration " +
            "interval=\"10000\" lifespan=\"10\" max-idle=\"10\"/></distributed-cache></cache-container></infinispan>",
            cacheName);
      cacheCreateWithStringConfiguration(cacheName, xml);
   }

   public void cacheCreateWithXMLFragmentConfigurationTest(Method m) {
      String cacheName = m.getName();
      String xml = String.format(
            "<distributed-cache name=\"%s\"><encoding><key media-type=\"text/plain\"/><value " +
            "media-type=\"application/json\"/></encoding><expiration interval=\"10000\" lifespan=\"10\" " +
            "max-idle=\"10\"/></distributed-cache>",
            cacheName);
      cacheCreateWithStringConfiguration(cacheName, xml);
   }

   public void cacheCreateWithJSONConfigurationTest(Method m) {
      String cacheName = m.getName();
      String json = "{\n" +
            "  \"distributed-cache\" : {\n" +
            "    \"encoding\" : {\n" +
            "      \"key\" : {\n" +
            "        \"media-type\" : \"text/plain\"\n" +
            "      },\n" +
            "      \"value\" : {\n" +
            "        \"media-type\" : \"application/json\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"expiration\" : {\n" +
            "      \"interval\" : \"10000\",\n" +
            "      \"lifespan\" : \"10\",\n" +
            "      \"max-idle\" : \"10\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";
      cacheCreateWithStringConfiguration(cacheName, json);
   }

   public void cacheCreateWithYAMLConfigurationTest(Method m) {
      String cacheName = m.getName();
      String yaml =
            "distributed-cache:\n" +
            "  encoding:\n" +
            "    key:\n"+
            "      media-type: text/plain\n"+
            "    value:\n"+
            "      media-type: application/json\n"+
            "  expiration:\n"+
            "    interval: 10000\n"+
            "    lifespan: 10\n"+
            "    max-idle: 10\n";
      cacheCreateWithStringConfiguration(cacheName, yaml);
   }

   private void cacheCreateWithStringConfiguration(String cacheName, String xml) {
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
               .getOrCreateCache(cacheName, new StringConfiguration(xml));
      Configuration configuration = manager(0).getCache(cacheName).getCacheConfiguration();
      assertEquals(10000, configuration.expiration().wakeUpInterval());
      assertEquals(10, configuration.expiration().lifespan());
      assertEquals(10, configuration.expiration().maxIdle());
      assertEquals(MediaType.TEXT_PLAIN, configuration.encoding().keyDataType().mediaType());
      assertEquals(MediaType.APPLICATION_JSON, configuration.encoding().valueDataType().mediaType());
   }

   public void cacheCreateWithXMLConfigurationAndGetCacheTest(Method m) {
      String cacheName = m.getName();
      String xml = String.format(
            "<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", cacheName);
      cacheCreateWithXMLConfigurationAndGetCache(cacheName, xml);
   }

   public void cacheCreateWithReducedXMLConfigurationAndGetCacheTest(Method m) {
      String cacheName = m.getName();
      String xml = String.format("<distributed-cache name=\"%s\"/>", cacheName);
      cacheCreateWithXMLConfigurationAndGetCache(cacheName, xml);
   }

   private void cacheCreateWithXMLConfigurationAndGetCache(String cacheName, String xml) {
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
               .createCache(cacheName, new StringConfiguration(xml));
      final RemoteCache<Object, Object> cache = client(0).getCache(cacheName);
      assertNotNull(cache);
   }

   public void cacheCreateWithEmbeddedConfigurationTest(Method m) {
      String cacheName = m.getName();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.expiration().wakeUpInterval(10000).maxIdle(10).lifespan(10);
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE)
               .getOrCreateCache(cacheName, builder.build());
      Configuration configuration = manager(0).getCache(cacheName).getCacheConfiguration();
      assertEquals(10000, configuration.expiration().wakeUpInterval());
      assertEquals(10, configuration.expiration().lifespan());
      assertEquals(10, configuration.expiration().maxIdle());
   }

   public void cacheReindexTest(Method m) {
      String cacheName = m.getName();
      // Create the cache
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, "template");
      RemoteCache<String, Transaction> cache = client(0).getCache(cacheName);
      verifyQuery(cache, 0);
      Transaction tx = new TransactionPB();
      tx.setId(1);
      tx.setAccountId(777);
      tx.setAmount(500);
      tx.setDate(new Date(1));
      tx.setDescription("February rent");
      tx.setLongDescription("February rent");
      tx.setNotes("card was not present");
      cache.withFlags(Flag.SKIP_INDEXING).put("tx", tx);
      verifyQuery(cache, 0);
      client(0).administration().reindexCache(cacheName);
      verifyQuery(cache, 1);
      client(0).administration().removeCache(cacheName);
   }

   public void updateIndexSchemaTest(Method m) {
      String cacheName = m.getName();
      // Create the cache
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, "template");
      RemoteCache<String, Transaction> cache = client(0).getCache(cacheName);
      verifyQuery(cache, 0);
      Transaction tx = new TransactionPB();
      tx.setId(1);
      tx.setAccountId(777);
      tx.setAmount(500);
      tx.setDate(new Date(1));
      tx.setDescription("February rent");
      tx.setLongDescription("February rent");
      tx.setNotes("card was not present");
      cache.put("tx", tx);
      verifyQuery(cache, 1);
      client(0).administration().updateIndexSchema(cacheName);
      verifyQuery(cache, 1);
      client(0).administration().removeCache(cacheName);
   }

   private void verifyQuery(RemoteCache<String, Transaction> cache, int count) {
      List<User> users = Search.getQueryFactory(cache)
                               .<User>create("from sample_bank_account.Transaction where longDescription:'RENT'")
                               .execute().list();
      assertEquals(count, users.size());
   }

   public void testGetCacheNames(Method m) {
      String cacheName = m.getName();
      client(0).administration().withFlags(CacheContainerAdmin.AdminFlag.VOLATILE).createCache(cacheName, "template");
      Set<String> cacheNames = client(0).getCacheNames();
      assertEquals(manager(0).getCacheNames(), cacheNames);
   }
}
