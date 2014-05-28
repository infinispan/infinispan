package org.infinispan.server.test.query;

import static org.infinispan.server.test.util.ITestUtils.SERVER1_MGMT_PORT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;

import java.net.URL;

import org.infinispan.arquillian.core.RunningServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.marshall.ProtoStreamMarshaller;
import org.infinispan.protostream.sampledomain.marshallers.MarshallerRegistration;
import org.infinispan.server.test.util.RemoteCacheManagerFactory;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.clustering.infinispan.subsystem.InfinispanExtension;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.client.ModelControllerClient;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;

/**
 * Tests for remote queries over HotRod but registering the proto file via JON/RHQ plugin.
 *
 * @author William Burns
 */
@RunWith(Arquillian.class)
@WithRunningServer({@RunningServer(name = "remote-query")})
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
      URL resource = getClass().getResource("/sample_bank_account/bank.protobin");
      ModelControllerClient client = ModelControllerClient.Factory.create(
            getServer().getHotrodEndpoint().getInetAddress().getHostName(), SERVER1_MGMT_PORT);

      ModelNode addProtobufFileOp = getOperation("local", "upload-proto-file", new ModelNode().add().set(
            "proto-url", resource.toString()));

      ModelNode result = client.execute(addProtobufFileOp);
      Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

      client.close();

      //initialize client-side serialization context
      MarshallerRegistration.registerMarshallers(ProtoStreamMarshaller.getSerializationContext(remoteCacheManager));
   }

   protected static PathAddress getCacheContainerAddress(String containerName) {
      return PathAddress.pathAddress(PathElement.pathElement(ModelDescriptionConstants.SUBSYSTEM,
                                                             InfinispanExtension.SUBSYSTEM_NAME)).append("cache-container", containerName);
   }

   protected static ModelNode getOperation(String containerName, String operationName, ModelNode arguments) {
      PathAddress cacheAddress = getCacheContainerAddress(containerName);
      return Util.getOperation(operationName, cacheAddress, arguments);
   }
}
