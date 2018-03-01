package org.infinispan.test.integration.as;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;
import static org.junit.Assert.assertEquals;

import java.io.IOException;

import javax.enterprise.context.ApplicationScoped;

import org.infinispan.Version;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.as.arquillian.api.ContainerResource;
import org.jboss.as.arquillian.container.ManagementClient;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.junit.runner.RunWith;

@ApplicationScoped
@RunWith(Arquillian.class)
public class InfinispanExtensionCliIT {

   @ContainerResource("container.no-ispn")
   ManagementClient managementClient;

   @Test
   public void testCliContainerCreation() throws IOException {
      assert managementClient != null;

      PathAddress extension = PathAddress.pathAddress("extension", "org.infinispan.extension:" + Version.getModuleSlot());
      PathAddress subsystem = PathAddress.pathAddress("subsystem", "datagrid-infinispan");
      PathAddress container = PathAddress.pathAddress(subsystem.append("cache-container", "testContainer"));
      PathAddress configurations = PathAddress.pathAddress(container.append("configurations", "CONFIGURATIONS"));
      PathAddress config = PathAddress.pathAddress(configurations.append("local-cache-configuration", "local"));

      assertAddSuccess(extension);
      assertAddSuccess(subsystem);
      assertAddSuccess(container);
      assertAddSuccess(configurations);
      assertAddSuccess(config);
   }

   private void assertAddSuccess(PathAddress address) throws IOException {
      executeAndAssert(Util.createAddOperation(address), SUCCESS);
   }

   private void executeAndAssert(ModelNode operation, String outcome) throws IOException {
      ModelNode result = managementClient.getControllerClient().execute(operation);
      assertEquals(result.asString(), outcome, result.get(OUTCOME).asString());
   }
}
