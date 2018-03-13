package org.infinispan.server.endpoint;

import static org.infinispan.server.endpoint.subsystem.ModelKeys.REST_CONNECTOR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.READ_RESOURCE_OPERATION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.RESULT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.infinispan.server.endpoint.subsystem.EndpointExtension;
import org.infinispan.server.endpoint.subsystem.ModelKeys;
import org.jboss.as.clustering.infinispan.subsystem.InfinispanSubsystemDependenciesInitialization;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.subsystem.test.AbstractSubsystemTest;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 9.2
 */
public class RestConnectorTestCase extends AbstractSubsystemTest {

   private KernelServices services;

   public RestConnectorTestCase() {
      super(Constants.SUBSYSTEM_NAME, new EndpointExtension());
   }

   protected String getSubsystemXml() throws IOException {
      return readResource("endpoint-9.2.xml");
   }

   @Before
   public void setUp() throws Exception {
      services = createKernelServicesBuilder(new InfinispanSubsystemDependenciesInitialization())
            .setSubsystemXml(getSubsystemXml()).build();
   }

   @Test
   public void testCompressionLevel() {
      assertCompressionLevelForConnector("rest1", 7);
   }

   private void assertCompressionLevelForConnector(String connectorName, int compressionLevel) {
      PathAddress address = PathAddress.pathAddress(Constants.SUBSYSTEM_PATH).append(REST_CONNECTOR, connectorName);

      ModelNode readOp = new ModelNode();
      readOp.get(OP).set(READ_RESOURCE_OPERATION);
      readOp.get(OP_ADDR).set(address.toModelNode());
      ModelNode opResult = services.executeOperation(readOp);

      assertEquals(SUCCESS, opResult.get(OUTCOME).asString());

      ModelNode restConnector = opResult.get(RESULT);

      ModelNode modelNode = restConnector.get(ModelKeys.COMPRESSION_LEVEL);
      assertEquals(compressionLevel, modelNode.asInt());
   }

}
