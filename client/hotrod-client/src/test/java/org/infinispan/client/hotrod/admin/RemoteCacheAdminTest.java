package org.infinispan.client.hotrod.admin;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Set;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.TransactionPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.configuration.XMLStringConfiguration;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.embedded.testdomain.Transaction;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
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
      builder.indexing().index(Index.ALL).autoConfig(true);
      createHotRodServers(2, builder);
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = super.createHotRodClientConfigurationBuilder(serverPort);
      builder.marshaller(new ProtoStreamMarshaller());
      return builder;
   }

   @Override
   protected HotRodServer addHotRodServer(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.defaultCacheName("default");
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);

      EmbeddedCacheManager cm = addClusterEnabledCacheManager(gcb, builder);
      cm.defineConfiguration("template", builder.build());
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder);
      servers.add(server);
      return server;
   }

   public void cacheCreateRemoveTest(Method m) {
      String cacheName = m.getName();
      client(0).administration().createCache(cacheName, "template");
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
      client(0).administration().createCache(cacheName, "template");
      client(0).administration().createCache(cacheName, "template");
   }

   public void getOrCreateWithTemplateTest(Method m) {
      String cacheName = m.getName();
      client(0).administration().createCache(cacheName, "template");
      client(0).administration().getOrCreateCache(cacheName, "template");
   }

   public void getOrCreateWithoutTemplateTest() {
      client(0).administration().getOrCreateCache("default", (String)null);
   }

   public void cacheCreateWithXMLConfigurationTest(Method m) {
      String cacheName = m.getName();
      String xml = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"><expiration interval=\"10000\" lifespan=\"10\" max-idle=\"10\"/></distributed-cache></cache-container></infinispan>", cacheName);
      client(0).administration().getOrCreateCache(cacheName, new XMLStringConfiguration(xml));
      Configuration configuration = manager(0).getCache(cacheName).getCacheConfiguration();
      assertEquals(10000, configuration.expiration().wakeUpInterval());
      assertEquals(10, configuration.expiration().lifespan());
      assertEquals(10, configuration.expiration().maxIdle());
   }

   public void cacheCreateWithXMLConfigurationAndGetCacheTest(Method m) {
      String cacheName = m.getName();
      String xml = String.format("<infinispan><cache-container><distributed-cache name=\"%s\"/></cache-container></infinispan>", cacheName);
      client(0).administration().createCache(cacheName, new XMLStringConfiguration(xml));
      final RemoteCache<Object, Object> cache = client(0).getCache(cacheName);
      assertNotNull(cache);
   }

   public void cacheCreateWithEmbeddedConfigurationTest(Method m) {
      String cacheName = m.getName();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.expiration().wakeUpInterval(10000).maxIdle(10).lifespan(10);
      client(0).administration().getOrCreateCache(cacheName, builder.build());
      Configuration configuration = manager(0).getCache(cacheName).getCacheConfiguration();
      assertEquals(10000, configuration.expiration().wakeUpInterval());
      assertEquals(10, configuration.expiration().lifespan());
      assertEquals(10, configuration.expiration().maxIdle());
   }

   public void cacheReindexTest(Method m) throws IOException {
      String cacheName = m.getName();
      //initialize server-side serialization
      RemoteCache<String, String> metadataCache = client(0).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));

      // Create the cache
      client(0).administration().createCache(cacheName, "template");
      RemoteCache<String, Transaction> cache = client(0).getCache(cacheName);
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

   private void verifyQuery(RemoteCache<String, Transaction> cache, int count) {
      List<User> users = Search.getQueryFactory(cache).create("from sample_bank_account.Transaction where longDescription:'RENT'").list();
      assertEquals(count, users.size());
   }

   public void testGetCacheNames(Method m) {
      String cacheName = m.getName();
      client(0).administration().createCache(cacheName, "template");
      Set<String> cacheNames = client(0).getCacheNames();
      assertEquals(manager(0).getCacheNames(), cacheNames);
   }
}
