package org.infinispan.client.hotrod.query;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.CacheException;
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
import org.infinispan.query.remote.ProtobufMetadataManagerMBean;
import org.infinispan.query.remote.indexing.ProtobufValueWrapper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;
import org.infinispan.server.hotrod.HotRodServer;

import javax.management.Attribute;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Testing the functionality of JMX operations for the Remote Queries.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.query.RemoteQueryJmxTest", groups = "functional")
@CleanupAfterMethod
public class RemoteQueryJmxTest extends SingleCacheManagerTest {
   public static final String JMX_DOMAIN = ProtobufMetadataManager.class.getSimpleName();

   public static final String TEST_CACHE_NAME = "userCache";

   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, User> remoteCache;
   private MBeanServer server = null;

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
            .indexing().enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", getLuceneDirectoryProvider())
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

      ObjectName objName = new ObjectName(JMX_DOMAIN + ":type=RemoteQuery,name="
                                                + ObjectName.quote("DefaultCacheManager")
                                                + ",component=" + ProtobufMetadataManager.OBJECT_NAME);

      byte[] descriptor = readClasspathResource("/bank.protobin");
      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ProtobufMetadataManagerMBean protobufMetadataManagerMBean = JMX.newMBeanProxy(mBeanServer, objName, ProtobufMetadataManagerMBean.class);
      protobufMetadataManagerMBean.registerProtofile(descriptor);

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();

      return cacheManager;
   }

   private byte[] readClasspathResource(String classPathResource) throws IOException {
      InputStream is = getClass().getResourceAsStream(classPathResource);
      return Util.readStream(is);
   }

   protected String getLuceneDirectoryProvider() {
      return "ram";
   }

   @AfterTest
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotRodServer);
   }

   public void testQueryStatsMBean() throws Exception {
      ObjectName name = getQueryStatsObjectName(JMX_DOMAIN, TEST_CACHE_NAME);
      assertTrue(server.isRegistered(name));
      assertFalse((Boolean) server.getAttribute(name, "StatisticsEnabled"));
      server.setAttribute(name, new Attribute("StatisticsEnabled", true));
      assertTrue((Boolean) server.getAttribute(name, "StatisticsEnabled"));
   }

   public void testEmbeddedAttributeQuery() throws Exception {
      ObjectName name = getQueryStatsObjectName(JMX_DOMAIN, TEST_CACHE_NAME);

      log.tracef("StatisticsEnabled=%s", server.getAttribute(name, "StatisticsEnabled"));

      server.setAttribute(name, new Attribute("StatisticsEnabled", true));

      remoteCache.put(1, createUser(1));
      remoteCache.put(2, createUser(2));

      // get user back from remote cache via query and check its attributes
      QueryFactory qf = Search.getQueryFactory(remoteCache);
      Query query = qf.from(User.class)
            .having("addresses.postCode").eq("1231").toBuilder()
            .build();
      List<User> list = query.list();
      assertNotNull(list);
      assertEquals(1, list.size());
      assertEquals(User.class, list.get(0).getClass());
      assertEquals("Tom1", list.get(0).getName());

      assertEquals(2, server.invoke(name, "getNumberOfIndexedEntities",
                                    new Object[]{ProtobufValueWrapper.class.getCanonicalName()},
                                    new String[]{String.class.getCanonicalName()}));

      Set<String> classNames = (Set<String>) server.getAttribute(name, "IndexedClassNames");
      assertEquals(1, classNames.size());
      assertTrue("The set should contain the ProtobufValueWrapper class name.", classNames.contains(ProtobufValueWrapper.class.getCanonicalName()));
      assertTrue("The query execution total time should be > 0.", (Long) server.getAttribute(name, "SearchQueryTotalTime") > 0);
      assertEquals((long) 1, server.getAttribute(name, "SearchQueryExecutionCount"));
   }

   private User createUser(int id) {
      User user = new User();
      user.setId(id);
      user.setName("Tom" + id);
      user.setSurname("Cat" + id);
      user.setGender(User.Gender.MALE);
      user.setAccountIds(Collections.singletonList(12));
      Address address = new Address();
      address.setStreet("Dark Alley");
      address.setPostCode("123" + id);
      user.setAddresses(Collections.singletonList(address));
      return user;
   }

   private ObjectName getQueryStatsObjectName(String jmxDomain, String cacheName) {
      String cacheManagerName = cacheManager.getCacheManagerConfiguration().globalJmxStatistics().cacheManagerName();
      try {
         return new ObjectName(jmxDomain + ":type=Query,manager=" + ObjectName.quote(cacheManagerName)
                                     + ",cache=" + ObjectName.quote(cacheName) + ",component=Statistics");
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Malformed object name", e);
      }
   }
}
