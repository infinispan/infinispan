package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.FAILED;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;

import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
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

        // add a cache container
        ModelNode result = servicesA.executeOperation(addContainerOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // add a local cache
        result = servicesA.executeOperation(addCacheOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // remove the cache container
        result = servicesA.executeOperation(removeContainerOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // add the same cache container
        result = servicesA.executeOperation(addContainerOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // add the same local cache
        result = servicesA.executeOperation(addCacheOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // remove the same local cache
        result = servicesA.executeOperation(removeCacheOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
    }

    @Test
    public void testCacheContainerRemoveRemoveSequence() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        ModelNode addContainerOp = getCacheContainerAddOperation("maximal2");
        ModelNode removeContainerOp = getCacheContainerRemoveOperation("maximal2");
        ModelNode addCacheOp = getCacheAddOperation("maximal2", ModelKeys.LOCAL_CACHE, "fred");

        // add a cache container
        ModelNode result = servicesA.executeOperation(addContainerOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // add a local cache
        result = servicesA.executeOperation(addCacheOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // remove the cache container
        result = servicesA.executeOperation(removeContainerOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // remove the cache container again
        result = servicesA.executeOperation(removeContainerOp);
        Assert.assertEquals(FAILED, result.get(OUTCOME).asString());
    }

    @Test
    public void testLocalCacheAddRemoveAddSequence() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        ModelNode addOp = getCacheAddOperation("maximal", ModelKeys.LOCAL_CACHE, "fred");
        ModelNode removeOp = getCacheRemoveOperation("maximal", ModelKeys.LOCAL_CACHE, "fred");

        // add a local cache
        ModelNode result = servicesA.executeOperation(addOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // remove the local cache
        result = servicesA.executeOperation(removeOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // add the same local cache
        result = servicesA.executeOperation(addOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
    }

    @Test
    public void testLocalCacheRemoveRemoveSequence() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        ModelNode addOp = getCacheAddOperation("maximal", ModelKeys.LOCAL_CACHE, "fred");
        ModelNode removeOp = getCacheRemoveOperation("maximal", ModelKeys.LOCAL_CACHE, "fred");

        // add a local cache
        ModelNode result = servicesA.executeOperation(addOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // remove the local cache
        result = servicesA.executeOperation(removeOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // remove the same local cache
        result = servicesA.executeOperation(removeOp);
        Assert.assertEquals(FAILED, result.get(OUTCOME).asString());
    }

}
