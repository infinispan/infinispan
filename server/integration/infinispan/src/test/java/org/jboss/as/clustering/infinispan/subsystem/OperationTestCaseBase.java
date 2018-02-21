package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.clustering.infinispan.subsystem.ModelKeys.DEFAULT_CACHE;
import static org.jboss.as.clustering.infinispan.subsystem.ModelKeys.JNDI_NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.NAME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.READ_ATTRIBUTE_OPERATION;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;

import org.jboss.as.controller.ControlledProcessState;
import org.jboss.as.controller.ModelVersion;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.model.test.ModelTestModelControllerService;
import org.jboss.as.subsystem.test.AbstractSubsystemTest;
import org.jboss.as.subsystem.test.AdditionalInitialization;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.as.subsystem.test.KernelServicesBuilder;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;

/**
* Base test case for testing management operations.
*
* @author Richard Achmatowicz (c) 2011 Red Hat Inc.
*/

public class OperationTestCaseBase extends AbstractSubsystemTest {

    static final ModelVersion VERSION = Namespace.CURRENT.getVersion();
    static final String SUBSYSTEM_XML_FILE = String.format("subsystem-infinispan_%d_%d.xml", VERSION.getMajor(), VERSION.getMinor());

    public OperationTestCaseBase() {
        super(InfinispanExtension.SUBSYSTEM_NAME, new InfinispanExtension());
    }

    KernelServicesBuilder createKernelServicesBuilder() {
       return this.createKernelServicesBuilder(this.createAdditionalInitialization());
    }

    AdditionalInitialization createAdditionalInitialization() {
       return new InfinispanSubsystemDependenciesInitialization();
    }

    // cache container access
    protected static ModelNode getCacheContainerAddOperation(String containerName) {
        // create the address of the cache
        PathAddress containerAddr = getCacheContainerAddress(containerName);
        ModelNode addOp = Util.createAddOperation(containerAddr);
        // required attributes
        addOp.get(DEFAULT_CACHE).set("default");
        return addOp ;
    }

    protected static ModelNode getCacheContainerReadOperation(String containerName, String name) {
        // create the address of the subsystem
        PathAddress transportAddress = getCacheContainerAddress(containerName);
        ModelNode readOp = new ModelNode() ;
        readOp.get(OP).set(READ_ATTRIBUTE_OPERATION);
        readOp.get(OP_ADDR).set(transportAddress.toModelNode());
        // required attributes
        readOp.get(NAME).set(name);
        return readOp ;
    }

    protected static ModelNode getCacheContainerWriteOperation(String containerName, String name, String value) {
        // create the address of the subsystem
        PathAddress cacheAddress = getCacheContainerAddress(containerName);
        return Util.getWriteAttributeOperation(cacheAddress, name, new ModelNode().set(value));
    }

    protected static ModelNode getCacheContainerRemoveOperation(String containerName) {
        // create the address of the cache
        PathAddress containerAddr = getCacheContainerAddress(containerName);
        return Util.createRemoveOperation(containerAddr);
    }

    // cache access
    protected static ModelNode getCacheAddOperation(String containerName, String cacheType, String cacheName) {
        // create the address of the cache
        PathAddress cacheAddr = getCacheAddress(containerName, cacheType, cacheName);
        ModelNode addOp = Util.createAddOperation(cacheAddr);
        // required attributes
        addOp.get(JNDI_NAME).set("java:/fred/was/here");
        return addOp ;
    }

    protected static ModelNode getCacheConfigurationReadOperation(String containerName, String cacheType, String cacheName, String name) {
        // create the address of the subsystem
        PathAddress cacheAddress = getCacheConfigurationAddress(containerName, cacheType, cacheName);
        ModelNode readOp = new ModelNode() ;
        readOp.get(OP).set(READ_ATTRIBUTE_OPERATION);
        readOp.get(OP_ADDR).set(cacheAddress.toModelNode());
        // required attributes
        readOp.get(NAME).set(name);
        return readOp ;
    }

    protected static ModelNode getCacheConfigurationWriteOperation(String containerName, String cacheType, String cacheName, String name, String value) {
        PathAddress cacheAddress = getCacheConfigurationAddress(containerName, cacheType, cacheName);
        return Util.getWriteAttributeOperation(cacheAddress, name, new ModelNode().set(value));
    }

    protected static ModelNode getCacheRemoveOperation(String containerName, String cacheType, String cacheName) {
        PathAddress cacheAddr = getCacheAddress(containerName, cacheType, cacheName);
        return Util.createRemoveOperation(cacheAddr) ;
    }

    protected static ModelNode getStringKeyedJDBCCacheStoreReadOperation(String containerName, String cacheType, String cacheName, String name) {
        // create the address of the subsystem
        PathAddress cacheAddress = getStringKeyedJDBCCacheStoreAddress(containerName, cacheType, cacheName);
        ModelNode readOp = new ModelNode() ;
        readOp.get(OP).set(READ_ATTRIBUTE_OPERATION);
        readOp.get(OP_ADDR).set(cacheAddress.toModelNode());
        // required attributes
        readOp.get(NAME).set(name);
        return readOp ;
    }

    protected static ModelNode getStringKeyedJDBCCacheStoreWriteOperation(String containerName, String cacheType, String cacheName, String name, String value) {
        PathAddress cacheStoreAddress = getStringKeyedJDBCCacheStoreAddress(containerName, cacheType, cacheName);
        return Util.getWriteAttributeOperation(cacheStoreAddress, name, new ModelNode().set(value));
    }

    protected static PathAddress getStringKeyedJDBCCacheStoreAddress(String containerName, String cacheType, String cacheName) {
        return getCacheConfigurationAddress(containerName, cacheType, cacheName).append(ModelKeys.STRING_KEYED_JDBC_STORE, ModelKeys.JDBC_STORE);
    }

    protected static PathAddress getCacheContainerAddress(String containerName) {
        return PathAddress.pathAddress(InfinispanExtension.SUBSYSTEM_PATH).append(ModelKeys.CACHE_CONTAINER, containerName);
    }

    protected static PathAddress getCacheAddress(String containerName, String cacheType, String cacheName) {
        return getCacheContainerAddress(containerName).append(cacheType, cacheName);
    }

    protected static PathAddress getCacheConfigurationAddress(String containerName, String cacheType, String cacheName) {
        return getCacheContainerAddress(containerName).append(ModelKeys.CONFIGURATIONS, ModelKeys.CONFIGURATIONS_NAME).append(cacheType, cacheName);
    }

    protected String getSubsystemXml() throws IOException {
        return readResource(SUBSYSTEM_XML_FILE) ;
    }

    protected void assertServerState(final KernelServices services, String expected) throws Exception {
        ModelTestModelControllerService controllerService = extractField(services, "controllerService");
        ControlledProcessState processState = extractField(controllerService, "processState");
        assertEquals(expected, processState.getState().toString());
    }

    protected void executeAndAssertOutcome(KernelServices service, ModelNode operation, String outcome) {
        ModelNode result = service.executeOperation(operation);
        Assert.assertEquals(result.asString(), outcome, result.get(OUTCOME).asString());
    }

    public static <T> T extractField(Object target, String fieldName) {
        return (T) extractField(target.getClass(), target, fieldName);
    }

    public static Object extractField(Class type, Object target, String fieldName) {
        while (true) {
            Field field;
            try {
                field = type.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (Exception e) {
                if (type.equals(Object.class)) {
                    e.printStackTrace();
                    return null;
                } else {
                    // try with superclass!!
                    type = type.getSuperclass();
                }
            }
        }
    }

}
