package org.infinispan.server.test.query;

import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.clustering.infinispan.subsystem.InfinispanExtension;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

/**
 * Tests for remote queries over HotRod but registering the proto file via JON/RHQ plugin.
 *
 * @author William Burns
 */
@Category({ Queries.class })
@RunWith(Arquillian.class)
public class RemoteQueryJONRegisterIT extends RemoteQueryIT {

   @Before
   public void setUp() throws Exception {
      jmxConnectionProvider = new MBeanServerConnectionProvider(getServer().getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);
      rcmFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(getServer().getHotrodEndpoint().getInetAddress().getHostName())
            .port(getServer().getHotrodEndpoint().getPort())
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = rcmFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(cacheName);

      //initialize server-side serialization context via JON/RHQ
      ModelNode nameList = new ModelNode()
              .add("/sample_bank_account/bank.proto");
      ModelNode urlList = new ModelNode()
              .add(getClass().getResource("/sample_bank_account/bank.proto").toString());

      ModelControllerClient client = ModelControllerClient.Factory.create(
            getServer().getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);

      ModelNode addProtobufFileOp = getOperation("clustered", "upload-proto-schemas", nameList, urlList);

      ModelNode result = client.execute(addProtobufFileOp);
      Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

      client.close();

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   protected PathAddress getCacheContainerAddress(String containerName) {
      return PathAddress.pathAddress(PathElement.pathElement(ModelDescriptionConstants.SUBSYSTEM,
                                                             InfinispanExtension.SUBSYSTEM_NAME)).append("cache-container", containerName);
   }

   protected ModelNode getOperation(String containerName, String operationName, ModelNode nameList, ModelNode urlList) {
      PathAddress cacheAddress = getCacheContainerAddress(containerName);
      ModelNode op = new ModelNode();
      op.get(OP).set(operationName);
      op.get(OP_ADDR).set(cacheAddress.toModelNode());
      op.get("file-names").set(nameList);
      op.get("file-urls").set(urlList);
      return op;
   }
}
