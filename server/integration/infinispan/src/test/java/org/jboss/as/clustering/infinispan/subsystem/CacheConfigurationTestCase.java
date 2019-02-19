package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.subsystem.test.AbstractSubsystemTest;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;
import static org.wildfly.common.Assert.assertTrue;

/**
 *
 */
public class CacheConfigurationTestCase extends AbstractSubsystemTest {

    public CacheConfigurationTestCase() {
        super(InfinispanExtension.SUBSYSTEM_NAME, new InfinispanExtension());

    }

    @Test
    public void testConfigurationAttrIsPreserved() throws XMLStreamException, IOException {

        List<ModelNode> expected = new ArrayList<>();
        PathAddress subsystemAddr = PathAddress.pathAddress(InfinispanExtension.SUBSYSTEM_PATH);
        expected.add(Util.createAddOperation(subsystemAddr));

        PathAddress containerAddr =  subsystemAddr.append(ModelKeys.CACHE_CONTAINER, "local");
        PathAddress jdbcCacheAddr = containerAddr.append(ModelKeys.LOCAL_CACHE, "jdbcCache");
        ModelNode addJdbcCacheOp = Util.createAddOperation(jdbcCacheAddr);
        addJdbcCacheOp.get(ModelKeys.CONFIGURATION).set("jdbc-cache-config");


        List<ModelNode> modelNodes = this.parse(readResource("cache-configuration-test.xml"));
        Assert.assertTrue( String.format("expected %s in list of parsed model nodes", addJdbcCacheOp), modelNodes.contains(addJdbcCacheOp));
    }
}
