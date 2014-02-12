package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.util.Util;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.dsl.QueryBuilder;
import org.infinispan.query.remote.ProtobufMetadataManagerMBean;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import javax.management.JMX;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests manual indexing in server.
 *
 * @author anistor@redhat.com
 */
@RunWith(Arquillian.class)
@WithRunningServer("remote-query")
public class ManualIndexingTest {

   private static final String CACHE_MANAGER_NAME = "local";
   private static final String CACHE_NAME = "testcache_manual";

   @InfinispanResource("remote-query")
   private RemoteInfinispanServer server;

   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Integer, User> remoteCache;
   private MBeanServerConnectionProvider provider;
   private RemoteCacheManagerFactory rcmFactory;

   @Before
   public void setUp() throws Exception {
      provider = new MBeanServerConnectionProvider(server.getHotrodEndpoint().getInetAddress().getHostName(), 9999);
      rcmFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(server.getHotrodEndpoint().getInetAddress().getHostName())
            .port(server.getHotrodEndpoint().getPort())
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = rcmFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(CACHE_NAME);

      //initialize server-side serialization context via JMX
      ObjectName protobufMetadataManagerName = new ObjectName("jboss.infinispan:type=RemoteQuery,name="
                                                                    + ObjectName.quote(CACHE_MANAGER_NAME)
                                                                    + ",component=ProtobufMetadataManager");
      ProtobufMetadataManagerMBean protobufMetadataManagerMBean = JMX.newMBeanProxy(provider.getConnection(),
                                                                                    protobufMetadataManagerName,
                                                                                    ProtobufMetadataManagerMBean.class);
      protobufMetadataManagerMBean.registerProtofile(readClasspathResource("/sample_bank_account/bank.protobin"));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   @After
   public void tearDown() {
      if (remoteCache != null) {
         remoteCache.clear();
      }
      if (rcmFactory != null) {
         rcmFactory.stopManagers();
      }
      rcmFactory = null;
   }

   @Test
   public void testManualIndexing() throws Exception {
      QueryBuilder qb = Search.getQueryFactory(remoteCache).from(User.class)
            .having("name").eq("Tom").toBuilder();

      User user = new User();
      user.setId(1);
      user.setName("Tom");
      user.setSurname("Cat");
      user.setGender(User.Gender.MALE);
      remoteCache.put(1, user);

      assertEquals(0, qb.build().list().size());

      //manual indexing
      ObjectName massIndexerName = new ObjectName("jboss.infinispan:type=Query,manager="
                                                        + ObjectName.quote(CACHE_MANAGER_NAME)
                                                        + ",cache=" + ObjectName.quote(CACHE_NAME)
                                                        + ",component=MassIndexer");
      provider.getConnection().invoke(massIndexerName, "start", null, null);

      List<User> list = qb.build().list();
      assertEquals(1, list.size());
      User foundUser = list.get(0);
      assertEquals(1, foundUser.getId());
      assertEquals("Tom", foundUser.getName());
      assertEquals("Cat", foundUser.getSurname());
      assertEquals(User.Gender.MALE, foundUser.getGender());
   }

   private byte[] readClasspathResource(String c) throws IOException {
      InputStream is = getClass().getResourceAsStream(c);
      try {
         return Util.readStream(is);
      } finally {
         if (is != null) {
            is.close();
         }
      }
   }
}
