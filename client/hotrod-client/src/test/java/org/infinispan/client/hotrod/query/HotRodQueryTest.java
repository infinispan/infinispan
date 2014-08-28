package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.MarshallerRegistration;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test query via Hot Rod on a LOCAL cache.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.HotRodQueryTest", groups = "functional")
public class HotRodQueryTest extends SingleCacheManagerTest {

   protected static final String JMX_DOMAIN = ProtobufMetadataManager.class.getSimpleName();

   protected static final String TEST_CACHE_NAME = "userCache";

   protected HotRodServer hotRodServer;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Integer, User> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.globalJmxStatistics()
            .enable()
            .allowDuplicateDomains(true)
            .jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(new PerThreadMBeanServerLookup());

      ConfigurationBuilder builder = getConfigurationBuilder();

      cacheManager = TestCacheManagerFactory.createCacheManager(gcb, new ConfigurationBuilder(), true);
      cacheManager.defineConfiguration(TEST_CACHE_NAME, builder.build());
      cache = cacheManager.getCache(TEST_CACHE_NAME);

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);

      //initialize server-side serialization
      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.put("sample_bank_account/bank.proto", read("/sample_bank_account/bank.proto"));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));

      return cacheManager;
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer()
            .keyEquivalence(ByteArrayEquivalence.INSTANCE)
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return builder;
   }

   private String read(String classPathResource) throws IOException {
      return Util.read(getClass().getResourceAsStream(classPathResource));
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   @BeforeClass(alwaysRun = true)
   protected void populateCache() {
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
      remoteCache.put(1, user1);

      User user2 = new UserPB();
      user2.setId(2);
      user2.setName("Adrian");
      user2.setSurname("Nistor");
      user2.setGender(User.Gender.MALE);
      Address address2 = new AddressPB();
      address2.setStreet("Old Street");
      address2.setPostCode("XYZ");
      user2.setAddresses(Collections.singletonList(address2));
      remoteCache.put(2, user2);
   }

   @Override
   protected void clearContent() {
      //Don't clear, this is destroying the index
   }

   public void testAttributeQuery() throws Exception {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertNotNull(fromCache);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .having("name").eq("Tom").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   public void testEmbeddedAttributeQuery() throws Exception {
      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .having("addresses.postCode").eq("1234").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertUser1(list.get(0));
   }

   @Test(enabled = false, expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*HQLLUCN000005:.*", description = "See https://issues.jboss.org/browse/ISPN-4423")
   public void testInvalidEmbeddedAttributeQuery() throws Exception {
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query q = qf.from(UserPB.class)
            .setProjection("addresses").build();

      //todo [anistor] it would be best if the problem would be detected early at build() instead at doing it at list()
      q.list();  // exception expected
   }

   public void testProjections() throws Exception {
      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser1(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .setProjection("name", "surname")
            .having("name").eq("Tom").toBuilder()
            .build();

      List<Object[]> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(Object[].class, list.get(0).getClass());
      assertEquals("Tom", list.get(0)[0]);
      assertEquals("Cat", list.get(0)[1]);
   }

   private void assertUser1(User user) {
      assertNotNull(user);
      assertEquals(1, user.getId());
      assertEquals("Tom", user.getName());
      assertEquals("Cat", user.getSurname());
      assertEquals(User.Gender.MALE, user.getGender());
      assertNotNull(user.getAccountIds());
      assertEquals(1, user.getAccountIds().size());
      assertTrue(user.getAccountIds().contains(12));
      assertNotNull(user.getAddresses());
      assertEquals(1, user.getAddresses().size());
      assertEquals("Dark Alley", user.getAddresses().get(0).getStreet());
      assertEquals("1234", user.getAddresses().get(0).getPostCode());
   }
}
