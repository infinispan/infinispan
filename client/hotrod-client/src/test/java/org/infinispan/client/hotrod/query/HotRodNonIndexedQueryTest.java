package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.sampledomain.Address;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(testName = "client.hotrod.query.HotRodNonIndexedQueryTest", groups = "functional")
@CleanupAfterMethod
public class HotRodNonIndexedQueryTest extends SingleCacheManagerTest {

   public static final String JMX_DOMAIN = ProtobufMetadataManager.class.getSimpleName();

   public static final String TEST_CACHE_NAME = "userCache";

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, User> remoteCache;

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

      ObjectName objName = new ObjectName(JMX_DOMAIN + ":type=RemoteQuery,name="
                                                + ObjectName.quote("DefaultCacheManager")
                                                + ",component=" + ProtobufMetadataManager.OBJECT_NAME);

      //initialize server-side serialization context via JMX
      byte[] descriptor = readClasspathResource("/sample_bank_account/bank.protobin");
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      mBeanServer.invoke(objName, "registerProtofile", new Object[]{descriptor}, new String[]{byte[].class.getName()});

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));

      return cacheManager;
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer()
            .keyEquivalence(ByteArrayEquivalence.INSTANCE);
      return builder;
   }

   private byte[] readClasspathResource(String classPathResource) throws IOException {
      InputStream is = getClass().getResourceAsStream(classPathResource);
      try {
         return Util.readStream(is);
      } finally {
         if (is != null) {
            is.close();
         }
      }
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testAttributeQuery() throws Exception {
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(User.class)
            .having("name").eq("Tom").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
      assertUser(list.get(0));
   }

   public void testEmbeddedAttributeQuery() throws Exception {
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(User.class)
            .having("addresses.postCode").eq("1234").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
      assertUser(list.get(0));
   }

   @Test(expectedExceptions = HotRodClientException.class)
   public void testInvalidEmbeddedAttributeQuery() throws Exception {
      QueryFactory qf = Search.getQueryFactory(remoteCache);

      Query q = qf.from(User.class)
            .setProjection("addresses").build();

      //todo [anistor] it would be best if the problem would be detected early at build() instead at doing it at list()
      q.list();
   }

   public void testProjections() throws Exception {
      remoteCache.put(1, createUser1());
      remoteCache.put(2, createUser2());

      // get user back from remote cache and check its attributes
      User fromCache = remoteCache.get(1);
      assertUser(fromCache);

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(User.class)
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

   private User createUser1() {
      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singletonList(12));
      Address address = new Address();
      address.setStreet("Dark Alley");
      address.setPostCode("1234");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private User createUser2() {
      User user = new User();
      user.setId(1);
      user.setName("Adrian");
      user.setSurname("Nistor");
      user.setGender(User.Gender.MALE);
      Address address = new Address();
      address.setStreet("Old Street");
      address.setPostCode("XYZ");
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private void assertUser(User user) {
      assertNotNull(user);
      assertEquals(1, user.getId());
      assertEquals("Tom", user.getName());
      assertEquals("Cat", user.getSurname());
      assertEquals(User.Gender.MALE, user.getGender());
      assertNotNull(user.getAccountIds());
      assertEquals(1, user.getAccountIds().size());
      assertEquals(12, user.getAccountIds().get(0).intValue());
      assertNotNull(user.getAddresses());
      assertEquals(1, user.getAddresses().size());
      assertEquals("Dark Alley", user.getAddresses().get(0).getStreet());
      assertEquals("1234", user.getAddresses().get(0).getPostCode());
   }
}
