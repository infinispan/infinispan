package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.FAILED;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.READ_RESOURCE_OPERATION;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;
import static org.jboss.as.controller.operations.common.Util.createAddOperation;

import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
import org.junit.Test;

/**
 * A simple test case to show that stores can be added via the persistence=PERSISTENCE element, or via
 * the alias of the old entry on the configurations element.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class PersistenceResourceTestCase extends OperationSequencesTestCase {

   @Test
   public void testStoreAddRemoveViaAlias() throws Exception {

      // Parse and install the XML into the controller
      String subsystemXml = getSubsystemXml() ;
      KernelServices service = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

      String container = "maximal2";
      PathAddress containerAddr = getCacheContainerAddress(container);
      PathAddress confParentAddr = containerAddr.append(ModelKeys.CONFIGURATIONS, ModelKeys.CONFIGURATIONS_NAME);
      PathAddress cacheConfAddres = getCacheConfigurationAddress(container, ModelKeys.LOCAL_CACHE_CONFIGURATION, "example");
      PathAddress persistenceAddr = cacheConfAddres.append(PersistenceConfigurationResource.PATH);
      PathAddress fs1ActualAddr = persistenceAddr.append(ModelKeys.FILE_STORE, "fs1");
      PathAddress fs2ActualAddr = persistenceAddr.append(ModelKeys.FILE_STORE, "fs2");
      PathAddress fs1Alias = cacheConfAddres.append(ModelKeys.FILE_STORE, "fs1");
      PathAddress fs2Alias = cacheConfAddres.append(ModelKeys.FILE_STORE, "fs2");

      executeAndAssertOutcome(service, createAddOperation(containerAddr), SUCCESS);
      executeAndAssertOutcome(service, createAddOperation(confParentAddr), SUCCESS);
      executeAndAssertOutcome(service, createAddOperation(cacheConfAddres), SUCCESS);
      executeAndAssertOutcome(service, createAddOperation(persistenceAddr), SUCCESS);
      executeAndAssertOutcome(service, createAddOperation(fs1ActualAddr), SUCCESS);
      executeAndAssertOutcome(service, createAddOperation(fs2Alias), SUCCESS);

      readFileStoreAndAssertOutcome(service, fs1ActualAddr, SUCCESS);
      readFileStoreAndAssertOutcome(service, fs1Alias, SUCCESS);
      readFileStoreAndAssertOutcome(service, fs2ActualAddr, SUCCESS);
      readFileStoreAndAssertOutcome(service, fs2Alias, SUCCESS);

      executeAndAssertOutcome(service, Util.createRemoveOperation(fs1Alias), SUCCESS);
      executeAndAssertOutcome(service, Util.createRemoveOperation(fs2ActualAddr), SUCCESS);

      readFileStoreAndAssertOutcome(service, fs1ActualAddr, FAILED);
      readFileStoreAndAssertOutcome(service, fs1Alias, FAILED);
      readFileStoreAndAssertOutcome(service, fs2ActualAddr, FAILED);
      readFileStoreAndAssertOutcome(service, fs2Alias, FAILED);
   }

   private void readFileStoreAndAssertOutcome(KernelServices service, PathAddress address, String outcome) {
      ModelNode readOp = new ModelNode();
      readOp.get(OP).set(READ_RESOURCE_OPERATION);
      readOp.get(OP_ADDR).set(address.toModelNode());
      ModelNode result = service.executeOperation(readOp);
      Assert.assertEquals(result.asString(), outcome, result.get(OUTCOME).asString());
   }
}
