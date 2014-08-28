package org.infinispan.server.test.query;

import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.Search;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.sampledomain.User;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.dsl.Query;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.infinispan.commons.util.Util.read;
import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.infinispan.server.test.util.ITestUtils.invokeOperation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for Remote Query descriptors propagation across nodes.
 *
 * @author gustavonalle
 */

@Category({ Queries.class })
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "clustered-indexless-descriptor-2")})
public class RemoteQueryDescriptorIT {

   @InfinispanResource("remote-query")
   RemoteInfinispanServer server1;

   @InfinispanResource("clustered-indexless-descriptor-2")
   RemoteInfinispanServer server2;

   public static final String MBEAN = "jboss.infinispan:type=RemoteQuery,name=\"clustered\",component=ProtobufMetadataManager";

   @Test
   public void testDescriptorPropagation() throws Exception {
      registerProtoOnServer1();
      assertRegisteredOn(server1);
      assertRegisteredOn(server2);
      populateCache();

      assertEquals(1, queryResultsIn(server1));
      assertEquals(1, queryResultsIn(server2));
   }

   private void registerProtoOnServer1() throws Exception {
      String[] fileNames = {"sample_bank_account/bank.proto"};
      String[] fileContents = {read(getClass().getResourceAsStream("/sample_bank_account/bank.proto"))};

      invoke(getJmxConnection(server1), "registerProtofiles", fileNames, fileContents);
   }

   private void assertRegisteredOn(RemoteInfinispanServer server) throws Exception {
      Object proto = invoke(getJmxConnection(server), "displayProtofile", "sample_bank_account/bank.proto");
     
      assertTrue(proto.toString().contains("message User"));
   }

   private Object invoke(MBeanServerConnectionProvider provider, String opName, Object... params) throws Exception {
      String[] types = new String[params.length];
      for (int i = 0; i < params.length; i++) {
         types[i] = params[i].getClass().getName();
      }
      return invokeOperation(provider, MBEAN, opName, params, types);
   }

   private MBeanServerConnectionProvider getJmxConnection(RemoteInfinispanServer server) {
      return new MBeanServerConnectionProvider(server.getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
   }

   private int queryResultsIn(RemoteInfinispanServer server) throws IOException {
      ConfigurationBuilder configurationBuilder = configurationBuilder(server);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManagerFactory().createManager(configurationBuilder);
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
      RemoteCache<Integer, User> remoteCache = remoteCacheManager.getCache("repl_descriptor");
      Query query = Search.getQueryFactory(remoteCache).from(User.class).build();

      return query.list().size();
   }

   private ConfigurationBuilder configurationBuilder(RemoteInfinispanServer server) {
      return new ConfigurationBuilder()
              .addServer()
              .host(server.getHotrodEndpoint().getInetAddress().getHostName())
              .port(server.getHotrodEndpoint().getPort())
              .marshaller(new ProtoStreamMarshaller());
   }

   private void populateCache() throws IOException {
      ConfigurationBuilder clientBuilder = configurationBuilder(server1);
      User user = new User();
      user.setId(0);
      user.setName("user1");
      user.setSurname("surname");
      RemoteCacheManager manager = new RemoteCacheManagerFactory().createManager(clientBuilder);
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(manager));

      RemoteCache<Object, Object> cache = manager.getCache("repl_descriptor");
      cache.put(1, user);
   }
}
