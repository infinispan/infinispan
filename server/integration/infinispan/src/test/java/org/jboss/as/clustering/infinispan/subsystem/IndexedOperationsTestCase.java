package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.PathAddress;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

public class IndexedOperationsTestCase extends OperationTestCaseBase {

   @Override
   protected String getSubsystemXml() throws IOException {
      return readResource("indexing-inheritance.xml") ;
   }

   @Test
   public void testIndexedCacheInheritance() throws Exception {
      // Parse and install the XML into the controller
      String subsystemXml = getSubsystemXml();
      KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();
      PathAddress address = getCacheConfigurationAddress("indexing", ModelKeys.REPLICATED_CACHE_CONFIGURATION, "booksCache").append(ModelKeys.INDEXING, ModelKeys.INDEXING_NAME);
      ModelNode readOp = new ModelNode() ;
      readOp.get(OP).set(READ_ATTRIBUTE_OPERATION);
      readOp.get(OP_ADDR).set(address.toModelNode());
      readOp.get(NAME).set(ModelKeys.INDEXING);

      ModelNode result = servicesA.executeOperation(readOp);
      Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
      Assert.assertEquals(Indexing.LOCAL.toString(), result.get(RESULT).asString());
   }
}
