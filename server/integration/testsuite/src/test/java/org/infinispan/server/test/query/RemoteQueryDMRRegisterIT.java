package org.infinispan.server.test.query;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.server.test.category.Queries;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.clustering.infinispan.subsystem.InfinispanExtension;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.dmr.ModelNode;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Tests for remote queries over HotRod but registering the proto file via DMR plugin.
 *
 * @author William Burns
 */
@Category(Queries.class)
@RunWith(Arquillian.class)
public class RemoteQueryDMRRegisterIT extends RemoteQueryIT {

   @Before
   @Override
   public void setUp() throws Exception {
      rcmFactory = new RemoteCacheManagerFactory();
      ConfigurationBuilder clientBuilder = new ConfigurationBuilder();
      clientBuilder.addServer()
            .host(getServer().getHotrodEndpoint().getInetAddress().getHostName())
            .port(getServer().getHotrodEndpoint().getPort())
            .marshaller(new ProtoStreamMarshaller());
      remoteCacheManager = rcmFactory.createManager(clientBuilder);
      remoteCache = remoteCacheManager.getCache(cacheName);

      //initialize server-side serialization context via DMR
      ModelNode nameList = new ModelNode()
            .add("/sample_bank_account/bank.proto");
      ModelNode urlList = new ModelNode()
            .add(getClass().getResource("/sample_bank_account/bank.proto").toString());

      ModelControllerClient client = ModelControllerClient.Factory.create(
            getServer().getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);

      ModelNode addProtobufFileOp = getOperation("clustered", "upload-proto-schemas", nameList, urlList);

      ModelNode result = client.execute(addProtobufFileOp);
      assertEquals(SUCCESS, result.get(OUTCOME).asString());

      client.close();

      RemoteCache<String, String> metadataCache = remoteCacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   private ModelNode getOperation(String containerName, String operationName, ModelNode nameList, ModelNode urlList) {
      PathAddress address = PathAddress.pathAddress(
            PathElement.pathElement(ModelDescriptionConstants.SUBSYSTEM, InfinispanExtension.SUBSYSTEM_NAME))
            .append("cache-container", containerName);
      ModelNode op = new ModelNode();
      op.get(OP).set(operationName);
      op.get(OP_ADDR).set(address.toModelNode());
      op.get("file-names").set(nameList);
      op.get("file-urls").set(urlList);
      return op;
   }
}
