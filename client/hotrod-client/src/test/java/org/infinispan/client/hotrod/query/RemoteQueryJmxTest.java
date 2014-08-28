package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.TestHelper;
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
import org.infinispan.query.remote.ProtobufMetadataManagerMBean;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import javax.management.*;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.junit.Assert.*;

/**
 * Testing the functionality of JMX operations for the Remote Queries.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(testName = "client.hotrod.query.RemoteQueryJmxTest", groups = "functional")
public class RemoteQueryJmxTest extends SingleCacheManagerTest {

   private static final String JMX_DOMAIN = ProtobufMetadataManager.class.getSimpleName();

   private static final String TEST_CACHE_NAME = "userCache";

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, User> remoteCache;
   private MBeanServer mBeanServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.globalJmxStatistics()
            .enable()
            .allowDuplicateDomains(true)
            .jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(new PerThreadMBeanServerLookup());

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.dataContainer()
            .keyEquivalence(ByteArrayEquivalence.INSTANCE)
            .valueEquivalence(ByteArrayEquivalence.INSTANCE)
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      cacheManager = TestCacheManagerFactory.createCacheManager(gcb, builder, true);
      cacheManager.defineConfiguration(TEST_CACHE_NAME, builder.build());
      cache = cacheManager.getCache(TEST_CACHE_NAME);

      hotRodServer = TestHelper.startHotRodServer(cacheManager);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort());
      clientBuilder.marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());

      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);

      mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();

      ProtobufMetadataManagerMBean protobufMetadataManagerMBean = JMX.newMBeanProxy(mBeanServer, getProtobufMetadataManagerObjectName(), ProtobufMetadataManagerMBean.class);
      protobufMetadataManagerMBean.registerProtofiles(
            new String[]{"sample_bank_account/bank.proto"},
            new String[]{read("/sample_bank_account/bank.proto")}
      );

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));

      return cacheManager;
   }

   private String read(String classPathResource) throws IOException {
      return Util.read(getClass().getResourceAsStream(classPathResource));
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testIndexAndQuery() throws Exception {
      ObjectName name = getQueryStatsObjectName(TEST_CACHE_NAME);
      assertTrue(mBeanServer.isRegistered(name));

      assertFalse((Boolean) mBeanServer.getAttribute(name, "StatisticsEnabled"));

      mBeanServer.setAttribute(name, new Attribute("StatisticsEnabled", true));

      assertTrue((Boolean) mBeanServer.getAttribute(name, "StatisticsEnabled"));

      remoteCache.put(1, createUser(1));
      remoteCache.put(2, createUser(2));

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(UserPB.class)
            .having("addresses.postCode").eq("1231").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(UserPB.class, list.get(0).getClass());
      assertEquals("Tom1", list.get(0).getName());

      assertEquals(2, mBeanServer.invoke(name, "getNumberOfIndexedEntities",
                                    new Object[]{ProtobufValueWrapper.class.getName()},
                                    new String[]{String.class.getName()}));

      Set<String> classNames = (Set<String>) mBeanServer.getAttribute(name, "IndexedClassNames");
      assertEquals(1, classNames.size());
      assertTrue("The set should contain the ProtobufValueWrapper class name.", classNames.contains(ProtobufValueWrapper.class.getName()));
      assertTrue("The query execution total time should be > 0.", (Long) mBeanServer.getAttribute(name, "SearchQueryTotalTime") > 0);
      assertEquals((long) 1, mBeanServer.getAttribute(name, "SearchQueryExecutionCount"));
   }

   private User createUser(int id) {
      User user = new UserPB();
      user.setId(id);
      user.setName("Tom" + id);
      user.setSurname("Cat" + id);
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singleton(12));
      Address address = new AddressPB();
      address.setStreet("Dark Alley");
      address.setPostCode("123" + id);
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private ObjectName getQueryStatsObjectName(String cacheName) throws MalformedObjectNameException {
      String cacheManagerName = cacheManager.getCacheManagerConfiguration().globalJmxStatistics().cacheManagerName();
      return new ObjectName(JMX_DOMAIN + ":type=Query,manager=" + ObjectName.quote(cacheManagerName)
                                  + ",cache=" + ObjectName.quote(cacheName) + ",component=Statistics");
   }

   private ObjectName getProtobufMetadataManagerObjectName() throws MalformedObjectNameException {
      String cacheManagerName = cacheManager.getCacheManagerConfiguration().globalJmxStatistics().cacheManagerName();
      return new ObjectName(JMX_DOMAIN + ":type=RemoteQuery,name="
                                  + ObjectName.quote(cacheManagerName)
                                  + ",component=" + ProtobufMetadataManager.OBJECT_NAME);
   }
}
