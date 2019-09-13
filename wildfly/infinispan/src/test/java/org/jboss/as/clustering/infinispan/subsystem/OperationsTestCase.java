package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OUTCOME;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.RESULT;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUCCESS;

import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
import org.junit.Test;

/**
 *  Test case for testing individual management operations.
 *
 *  These test cases are based on the XML config in subsystem-infinispan-test,
 *  a non-exhaustive subsystem configuration.
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
*/
public class OperationsTestCase extends OperationTestCaseBase {

    // cache container test operations
    static final ModelNode readCacheContainerDefaultCacheOp = getCacheContainerReadOperation("maximal", "default-cache");
    static final ModelNode writeCacheContainerDefaultCacheOp = getCacheContainerWriteOperation("maximal", "default-cache", "new-default-cache");

    // cache test operations
    static final ModelNode readLocalCacheBatchingOp = getCacheConfigurationReadOperation("maximal", ModelKeys.LOCAL_CACHE_CONFIGURATION, "local", "batching");
    static final ModelNode writeLocalCacheBatchingOp = getCacheConfigurationWriteOperation("maximal", ModelKeys.LOCAL_CACHE_CONFIGURATION, "local", "batching", "false");

    // cache store test operations
    static final ModelNode readDistCacheStringJDBCStoreDatastoreOp = getStringKeyedJDBCCacheStoreReadOperation("maximal", ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION, "dist", "datasource");
    static final ModelNode writeDistCacheStringJDBCStoreDatastoreOp = getStringKeyedJDBCCacheStoreWriteOperation("maximal", ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION, "dist", "datasource", "new-datasource");
    static final ModelNode readDistCacheStringJDBCStoreStringKeyedTableOp = getStringKeyedJDBCCacheStoreReadOperation("maximal", ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION, "dist", "string-keyed-table");

    /*
     * Tests access to cache container attributes
     */
    @Test
    public void testCacheContainerReadWriteOperation() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        // read the cache container default cache attribute
        ModelNode result = servicesA.executeOperation(readCacheContainerDefaultCacheOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
        Assert.assertEquals("local", result.get(RESULT).asString());

        // write the default cache attribute
        result = servicesA.executeOperation(writeCacheContainerDefaultCacheOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // re-read the default cache attribute
        result = servicesA.executeOperation(readCacheContainerDefaultCacheOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
        Assert.assertEquals("new-default-cache", result.get(RESULT).asString());
    }

    /*
     * Tests access to local cache attributes
     */
    @Test
    public void testLocalCacheReadWriteOperation() throws Exception {

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        // read the cache container batching attribute
        ModelNode result = servicesA.executeOperation(readLocalCacheBatchingOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
        Assert.assertEquals("false", result.get(RESULT).asString());

        // write the batching attribute
        result = servicesA.executeOperation(writeLocalCacheBatchingOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // re-read the batching attribute
        result = servicesA.executeOperation(readLocalCacheBatchingOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
        Assert.assertEquals("false", result.get(RESULT).asString());
        assertServerState(servicesA, "running");
    }
    /*
     * Tests access to local cache attributes
     */
    @Test
    public void testDistributedCacheStringJDBCStoreReadWriteOperation() throws Exception {

        ModelNode stringKeyedTable = createStringKeyedTable() ;

        // Parse and install the XML into the controller
        String subsystemXml = getSubsystemXml() ;
        KernelServices servicesA = createKernelServicesBuilder().setSubsystemXml(subsystemXml).build();

        // read the distributed cache string-keyed-jdbc-store datasource attribute
        ModelNode result = servicesA.executeOperation(readDistCacheStringJDBCStoreDatastoreOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
        Assert.assertEquals("java:jboss/datasources/JdbcDS", result.get(RESULT).asString());

        // write the batching attribute
        result = servicesA.executeOperation(writeDistCacheStringJDBCStoreDatastoreOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());

        // re-read the batching attribute
        result = servicesA.executeOperation(readDistCacheStringJDBCStoreDatastoreOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
        Assert.assertEquals("new-datasource", result.get(RESULT).asString());

         // read the string-keyed-table attribute
        result = servicesA.executeOperation(readDistCacheStringJDBCStoreStringKeyedTableOp);
        Assert.assertEquals(SUCCESS, result.get(OUTCOME).asString());
        Assert.assertEquals(stringKeyedTable.asString(), result.get(RESULT).asString());
        assertServerState(servicesA, "running");
    }

    private ModelNode createStringKeyedTable() {
        // create a string-keyed-table complex attribute
        ModelNode stringKeyedTable = new ModelNode().setEmptyObject() ;
        stringKeyedTable.get(ModelKeys.PREFIX).set("ISPN_MC_SK");
        stringKeyedTable.get(ModelKeys.CREATE_ON_START).set(true);
        stringKeyedTable.get(ModelKeys.DROP_ON_EXIT).set(true);

        ModelNode idColumn = stringKeyedTable.get(ModelKeys.ID_COLUMN).setEmptyObject();
        idColumn.get(ModelKeys.NAME).set("id") ;
        idColumn.get(ModelKeys.TYPE).set("VARCHAR") ;

        ModelNode dataColumn = stringKeyedTable.get(ModelKeys.DATA_COLUMN).setEmptyObject();
        dataColumn.get(ModelKeys.NAME).set("datum") ;
        dataColumn.get(ModelKeys.TYPE).set("BINARY") ;

        ModelNode timestampColumn = stringKeyedTable.get(ModelKeys.TIMESTAMP_COLUMN).setEmptyObject();
        timestampColumn.get(ModelKeys.NAME).set("version") ;
        timestampColumn.get(ModelKeys.TYPE).set("BIGINT") ;

        return stringKeyedTable;
    }
}
