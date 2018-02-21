package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.FAILED;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;

import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.dmr.ModelNode;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
* Test case for testing sequences of management operations.
*
* @author Richard Achmatowicz (c) 2011 Red Hat Inc.
*/
@RunWith(BMUnitRunner.class)
public class OperationSequencesTestCase extends OperationTestCaseBase {

    @Test
    public void testCacheContainerAddRemoveAddSequence() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        ModelNode addContainerOp = getCacheContainerAddOperation("maximal2");
        ModelNode removeContainerOp = getCacheContainerRemoveOperation("maximal2");
        ModelNode addCacheOp = getCacheAddOperation("maximal2",  ModelKeys.LOCAL_CACHE, "fred");
        ModelNode removeCacheOp = getCacheRemoveOperation("maximal2", ModelKeys.LOCAL_CACHE, "fred");

        executeAndAssertOutcome(servicesA, addContainerOp, SUCCESS); // add a cache container
        executeAndAssertOutcome(servicesA, addCacheOp, SUCCESS); // add a local cache
        executeAndAssertOutcome(servicesA, removeContainerOp, SUCCESS); // remove the cache container
        executeAndAssertOutcome(servicesA, addContainerOp, SUCCESS); // add the same cache container
        executeAndAssertOutcome(servicesA, addCacheOp, SUCCESS); // add the same local cache
        executeAndAssertOutcome(servicesA, removeCacheOp, SUCCESS); // remove the same local cache
    }

    @Test
    public void testCacheContainerRemoveRemoveSequence() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        ModelNode addContainerOp = getCacheContainerAddOperation("maximal2");
        ModelNode removeContainerOp = getCacheContainerRemoveOperation("maximal2");
        ModelNode addCacheOp = getCacheAddOperation("maximal2", ModelKeys.LOCAL_CACHE, "fred");

        executeAndAssertOutcome(servicesA, addContainerOp, SUCCESS); // add a cache container
        executeAndAssertOutcome(servicesA, addCacheOp, SUCCESS); // add a local cache
        executeAndAssertOutcome(servicesA, removeContainerOp, SUCCESS); // remove the cache container
        executeAndAssertOutcome(servicesA, removeContainerOp, FAILED); // remove the cache container again
    }

    @Test
    public void testLocalCacheAddRemoveAddSequence() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        ModelNode addOp = getCacheAddOperation("maximal", ModelKeys.LOCAL_CACHE, "fred");
        ModelNode removeOp = getCacheRemoveOperation("maximal", ModelKeys.LOCAL_CACHE, "fred");

        executeAndAssertOutcome(servicesA, addOp, SUCCESS); // add a local cache
        executeAndAssertOutcome(servicesA, removeOp, SUCCESS); // remove the local cache
        executeAndAssertOutcome(servicesA, addOp, SUCCESS); // add the same local cache
    }

    @Test
    public void testLocalCacheRemoveRemoveSequence() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        ModelNode addOp = getCacheAddOperation("maximal", ModelKeys.LOCAL_CACHE, "fred");
        ModelNode removeOp = getCacheRemoveOperation("maximal", ModelKeys.LOCAL_CACHE, "fred");

        executeAndAssertOutcome(servicesA, addOp, SUCCESS); // add a local cache
        executeAndAssertOutcome(servicesA, removeOp, SUCCESS); // remove the local cache
        executeAndAssertOutcome(servicesA, removeOp, FAILED); // remove the same local cache
    }
}
