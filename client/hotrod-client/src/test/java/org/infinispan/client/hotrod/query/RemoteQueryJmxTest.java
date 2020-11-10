package org.infinispan.client.hotrod.query;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.management.Attribute;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.query.testdomain.protobuf.AddressPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.UserPB;
import org.infinispan.client.hotrod.query.testdomain.protobuf.marshallers.TestDomainSCI;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.testdomain.Address;
import org.infinispan.query.dsl.embedded.testdomain.User;
import org.infinispan.query.remote.client.ProtobufMetadataManagerMBean;
import org.infinispan.query.remote.impl.indexing.ProtobufValueWrapper;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Testing the functionality of JMX operations for the Remote Queries.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
// TODO HSEARCH-3129 Restore support for statistics
@Test(testName = "client.hotrod.query.RemoteQueryJmxTest", groups = "functional", enabled = false)
public class RemoteQueryJmxTest extends SingleCacheManagerTest {

   private static final String TEST_CACHE_NAME = "userCache";

   private static final String JMX_DOMAIN = RemoteQueryJmxTest.class.getSimpleName();
   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   private final MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, User> remoteCache;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.jmx().enabled(true)
         .domain(JMX_DOMAIN)
         .mBeanServerLookup(mBeanServerLookup);
      gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);

      cacheManager = TestCacheManagerFactory.createCacheManager(gcb, null);
      hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);

      ProtobufMetadataManagerMBean protobufMetadataManagerMBean = JMX.newMBeanProxy(mBeanServer, getProtobufMetadataManagerObjectName(), ProtobufMetadataManagerMBean.class);
      String protofile = Util.getResourceAsString("/sample_bank_account/bank.proto", getClass().getClassLoader());
      protobufMetadataManagerMBean.registerProtofile("sample_bank_account/bank.proto", protofile);
      assertEquals(protofile, protobufMetadataManagerMBean.getProtofile("sample_bank_account/bank.proto"));
      assertNull(protobufMetadataManagerMBean.getFilesWithErrors());
      assertTrue(Arrays.asList(protobufMetadataManagerMBean.getProtofileNames()).contains("sample_bank_account/bank.proto"));

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("sample_bank_account.User");
      cacheManager.defineConfiguration(TEST_CACHE_NAME, builder.build());
      cache = cacheManager.getCache(TEST_CACHE_NAME);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("127.0.0.1").port(hotRodServer.getPort()).addContextInitializer(TestDomainSCI.INSTANCE);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache(TEST_CACHE_NAME);
      return cacheManager;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      remoteCacheManager = null;
      killServers(hotRodServer);
      hotRodServer = null;
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
      Query<User> query = qf.create("FROM sample_bank_account.User u WHERE u.addresses.postCode = '1231'");
      List<User> list = query.execute().list();
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
      String cacheManagerName = cacheManager.getCacheManagerConfiguration().cacheManagerName();
      return new ObjectName(JMX_DOMAIN + ":type=Query,manager=" + ObjectName.quote(cacheManagerName)
            + ",cache=" + ObjectName.quote(cacheName) + ",component=Statistics");
   }

   private ObjectName getProtobufMetadataManagerObjectName() throws MalformedObjectNameException {
      String cacheManagerName = cacheManager.getCacheManagerConfiguration().cacheManagerName();
      return new ObjectName(JMX_DOMAIN + ":type=RemoteQuery,name="
            + ObjectName.quote(cacheManagerName)
            + ",component=" + ProtobufMetadataManagerMBean.OBJECT_NAME);
   }
}
