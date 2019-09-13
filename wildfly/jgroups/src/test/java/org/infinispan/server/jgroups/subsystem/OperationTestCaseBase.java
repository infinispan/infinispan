package org.infinispan.server.jgroups.subsystem;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.infinispan.server.commons.controller.Operations;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.subsystem.test.AbstractSubsystemTest;
import org.jboss.as.subsystem.test.AdditionalInitialization;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.dmr.ModelNode;

/**
* Base test case for testing management operations.
*
* @author Richard Achmatowicz (c) 2011 Red Hat Inc.
*/
public class OperationTestCaseBase extends AbstractSubsystemTest {

    static final String SUBSYSTEM_XML_FILE = JGroupsSchema.CURRENT.format("subsystem-%s-%d_%d.xml").replaceAll(":", "_");

    public OperationTestCaseBase() {
        super(JGroupsExtension.SUBSYSTEM_NAME, new JGroupsExtension());
    }

    protected static ModelNode getSubsystemReadOperation(String name) {
        return Operations.createReadAttributeOperation(getSubsystemAddress(), name);
    }

    protected static ModelNode getSubsystemWriteOperation(String name, String value) {
        return Operations.createWriteAttributeOperation(getSubsystemAddress(), name, new ModelNode(value));
    }

    protected static ModelNode getProtocolStackAddOperation(String stackName) {
        return Util.createAddOperation(getProtocolStackAddress(stackName));
    }

    protected static ModelNode getProtocolStackAddOperationWithParameters(String stackName) {
        ModelNode[] operations = new ModelNode[] {
                getProtocolStackAddOperation(stackName),
                getTransportAddOperation(stackName, "UDP"),
                getProtocolAddOperation(stackName, "MPING"),
                getProtocolAddOperation(stackName, "pbcast.FLUSH"),
        };
        return Operations.createCompositeOperation(operations);
    }

    protected static ModelNode getProtocolStackRemoveOperation(String stackName) {
        return Util.createRemoveOperation(getProtocolStackAddress(stackName));
    }

    protected static ModelNode getTransportAddOperation(String stackName, String protocol) {
        return Util.createAddOperation(getTransportAddress(stackName, protocol));
    }

    protected static ModelNode getTransportReadOperation(String stackName, String type, String name) {
        return Operations.createReadAttributeOperation(getTransportAddress(stackName, type), name);
    }

    protected static ModelNode getTransportWriteOperation(String stackName, String type, String name, String value) {
        return Operations.createWriteAttributeOperation(getTransportAddress(stackName, type), name, new ModelNode(value));
    }

    protected static ModelNode getTransportGetPropertyOperation(String stackName, String type, String propertyName) {
        return Operations.createMapGetOperation(getTransportAddress(stackName, type), ProtocolResourceDefinition.PROPERTIES.getName(), propertyName);
    }

    protected static ModelNode getTransportPutPropertyOperation(String stackName, String type, String propertyName, String propertyValue) {
        return Operations.createMapPutOperation(getTransportAddress(stackName, type), ProtocolResourceDefinition.PROPERTIES.getName(), propertyName, propertyValue);
    }

    protected static ModelNode getTransportRemovePropertyOperation(String stackName, String type, String propertyName) {
        return Operations.createMapRemoveOperation(getTransportAddress(stackName, type), ProtocolResourceDefinition.PROPERTIES.getName(), propertyName);
    }

    protected static ModelNode getProtocolAddOperation(String stackName, String type) {
        return Util.createAddOperation(getProtocolAddress(stackName, type));
    }

    protected static ModelNode getProtocolReadOperation(String stackName, String protocolName, String name) {
        return Operations.createReadAttributeOperation(getProtocolAddress(stackName, protocolName), name);
    }

    protected static ModelNode getProtocolWriteOperation(String stackName, String protocolName, String name, String value) {
        return Operations.createWriteAttributeOperation(getProtocolAddress(stackName, protocolName), name, new ModelNode(value));
    }

    protected static ModelNode getProtocolGetPropertyOperation(String stackName, String protocolName, String propertyName) {
        return Operations.createMapGetOperation(getProtocolAddress(stackName, protocolName), ProtocolResourceDefinition.PROPERTIES.getName(), propertyName);
    }

    protected static ModelNode getProtocolPutPropertyOperation(String stackName, String protocolName, String propertyName, String propertyValue) {
        return Operations.createMapPutOperation(getProtocolAddress(stackName, protocolName), ProtocolResourceDefinition.PROPERTIES.getName(), propertyName, propertyValue);
    }

    protected static ModelNode getProtocolRemovePropertyOperation(String stackName, String protocolName, String propertyName) {
        return Operations.createMapRemoveOperation(getProtocolAddress(stackName, protocolName), ProtocolResourceDefinition.PROPERTIES.getName(), propertyName);
    }

    protected static PathAddress getSubsystemAddress() {
        return PathAddress.pathAddress(JGroupsSubsystemResourceDefinition.PATH);
    }

    protected static PathAddress getProtocolStackAddress(String stackName) {
        return getSubsystemAddress().append(StackResourceDefinition.pathElement(stackName));
    }

    protected static PathAddress getTransportAddress(String stackName, String type) {
        return getProtocolStackAddress(stackName).append(TransportResourceDefinition.pathElement(type));
    }

    protected static PathAddress getProtocolAddress(String stackName, String type) {
        return getProtocolStackAddress(stackName).append(ProtocolResourceDefinition.pathElement(type));
    }

    protected String getSubsystemXml() throws IOException {
        return readResource(SUBSYSTEM_XML_FILE) ;
    }

    protected KernelServices buildKernelServices() throws XMLStreamException, IOException, Exception {
        return createKernelServicesBuilder(AdditionalInitialization.MANAGEMENT).setSubsystemXml(this.getSubsystemXml()).build();
    }
}
