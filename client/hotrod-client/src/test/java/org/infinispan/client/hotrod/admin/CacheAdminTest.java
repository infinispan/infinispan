package org.infinispan.client.hotrod.admin;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.admin.CacheAdminTest")
public class CacheAdminTest extends MultiHotRodServersTest {

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
      EmbeddedCacheManager cm = addClusterEnabledCacheManager(builder);
      cm.defineConfiguration("template", builder.build());
      HotRodServerConfigurationBuilder serverBuilder = new HotRodServerConfigurationBuilder();
      serverBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cm, serverBuilder);
      servers.add(server);
      return server;
   }

   public void cacheCreateRemoveTest() {
      client(0).administration().createCache("newCache", "template");
      assertTrue(manager(0).cacheExists("newCache"));
      assertTrue(manager(1).cacheExists("newCache"));
      client(1).administration().removeCache("newCache");
      assertFalse(manager(0).cacheExists("newCache"));
      assertFalse(manager(1).cacheExists("newCache"));
   }

   public void cacheReindexTest() throws IOException {
      //initialize server-side serialization
      RemoteCache<String, String> metadataCache = client(0).getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader()));
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(client(0)));

      // Create the cache
      client(0).administration().createCache("newCache", "template");
      RemoteCache<Object, Object> cache = client(0).getCache("newCache");
      User user1 = new UserPB();
      user1.setId(1);
      user1.setName("Tom");
      user1.setSurname("Cat");
      user1.setGender(User.Gender.MALE);
      user1.setAccountIds(Collections.singleton(12));
      Address address1 = new AddressPB();
      address1.setStreet("Dark Alley");
      address1.setPostCode("1234");
      user1.setAddresses(Collections.singletonList(address1));
      cache.withFlags(Flag.SKIP_INDEXING).put(0, user1);
      verifyUserQuery(cache, 0);
      client(0).administration().reindexCache("newCache");
      verifyUserQuery(cache, 1);
      client(0).administration().removeCache("newCache");
   }

   private void verifyUserQuery(RemoteCache<Object, Object> cache, int count) {
      List<User> users = Search.getQueryFactory(cache).create("from sample_bank_account.User where name='Tom'").list();
      assertEquals(count, users.size());
   }
}
