package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.client.helpers.ClientConstants.ADD;
import static org.jboss.as.controller.client.helpers.ClientConstants.COMPOSITE;
import static org.jboss.as.controller.client.helpers.ClientConstants.STEPS;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.READ_ATTRIBUTE_OPERATION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.RESULT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;

import java.io.IOException;

import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.client.helpers.ClientConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
import org.junit.Test;

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

   @Test
   public void testIndexingPropertiesNoRestart() throws Exception {

      // Parse and install the XML into the controller
      String subsystemXml = getSubsystemXml() ;
      KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

      final ModelNode composite = new ModelNode();
      composite.get(ClientConstants.OP).set(COMPOSITE);
      composite.get(ClientConstants.OP_ADDR).setEmptyList();

      final ModelNode steps = composite.get(STEPS);

      final ModelNode configAddOp = steps.add();
      configAddOp.get(ClientConstants.OP).set(ADD);

      configAddOp.get(ClientConstants.OP_ADDR).set(PathAddress.pathAddress(InfinispanExtension.SUBSYSTEM_PATH)
            .append("cache-container", "indexing")
            .append("configurations", "CONFIGURATIONS")
            .append("local-cache-configuration", "manualIndexingCacheConfig").toModelNode());
      configAddOp.get("template").set(true);

      PathAddress indexingAddress = PathAddress.pathAddress(configAddOp.get(ClientConstants.OP_ADDR)).append(ModelKeys.INDEXING, ModelKeys.INDEXING_NAME);
      ModelNode indexingAdd = Util.createAddOperation(indexingAddress);
      indexingAdd.get(ModelKeys.INDEXING).set("LOCAL");
      indexingAdd.get(ModelKeys.INDEXING_PROPERTIES).get("default.directory_provider").set("ram");
      indexingAdd.get(ModelKeys.INDEXING_PROPERTIES).get("hibernate.search.jmx_enabled").set("true");
      indexingAdd.get(ModelKeys.INDEXING_PROPERTIES).get("lucene_version").set("LUCENE_CURRENT");
      indexingAdd.get(ModelKeys.INDEXING_PROPERTIES).get("hibernate.search.indexing_strategy").set("manual");
      steps.add(indexingAdd);


      final ModelNode cacheAddOp = steps.add();
      cacheAddOp.get(ClientConstants.OP).set(ADD);
      cacheAddOp.get(ClientConstants.OP_ADDR).set(PathAddress.pathAddress(InfinispanExtension.SUBSYSTEM_PATH)
            .append("cache-container", "indexing")
            .append("local-cache", "manualIndexingCache").toModelNode());
      cacheAddOp.get("configuration").set("manualIndexingCacheConfig");

      ModelNode result = servicesA.executeOperation(composite);
      Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

      assertServerState(servicesA, "running");
   }
}
