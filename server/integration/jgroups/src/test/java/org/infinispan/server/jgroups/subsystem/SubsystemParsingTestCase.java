/*
* JBoss, Home of Professional Open Source.
* Copyright 2011, Red Hat Middleware LLC, and individual contributors
* as indicated by the @author tags. See the copyright.txt file in the
* distribution for a full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/
package org.infinispan.server.jgroups.subsystem;

import static org.junit.Assert.assertTrue;

import java.io.FileReader;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.xml.stream.XMLStreamException;

import org.infinispan.Version;
import org.infinispan.server.commons.controller.Operations;
import org.infinispan.server.commons.subsystem.ClusteringSubsystemTest;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.subsystem.test.AdditionalInitialization;
import org.jboss.as.subsystem.test.KernelServices;
import org.jboss.as.subsystem.test.KernelServicesBuilder;
import org.jboss.as.subsystem.test.ModelDescriptionValidator.ValidationConfiguration;
import org.jboss.dmr.ModelNode;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Tests parsing / booting / marshalling of JGroups configurations.
 *
 * The current XML configuration is tested, along with supported legacy configurations.
 *
 * @author <a href="kabir.khan@jboss.com">Kabir Khan</a>
 * @author Richard Achmatowicz (c) 2013 Red Hat Inc.
 */
@RunWith(value = Parameterized.class)
public class SubsystemParsingTestCase extends ClusteringSubsystemTest {

    private final int expectedOperationCount;
    private final String xsdPath;
    private final String[] templates;

    public SubsystemParsingTestCase(Path xmlPath, Properties properties) {
        super(JGroupsExtension.SUBSYSTEM_NAME, new JGroupsExtension(), xmlPath.getFileName().toString());
        this.expectedOperationCount = Integer.parseInt(properties.getProperty("expected.operations.count"));
        this.xsdPath = properties.getProperty("xsd.path");
        this.templates = null;
    }

    @Parameters
    public static Collection<Object[]> data() throws Exception {
        URL configDir = Thread.currentThread().getContextClassLoader().getResource("org/infinispan/server/jgroups/subsystem");
        List<Path> paths = Files.list(Paths.get(configDir.toURI()))
              .filter(path -> path.toString().endsWith(".xml"))
              .collect(Collectors.toList());
        boolean hasCurrentSchema = false;
        String currentSchema = "subsystem-infinispan_server_jgroups-" + Version.getSchemaVersion().replaceAll("\\.", "_") + ".xml";
        List<Object[]> data = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            Path xmlPath = paths.get(i);
            if (xmlPath.getFileName().toString().equals(currentSchema)) {
                hasCurrentSchema = true;
            }
            String propsPath = xmlPath.toString().replaceAll("\\.xml$", ".properties");
            Properties properties = new Properties();
            try (Reader r = new FileReader(propsPath)) {
                properties.load(r);
            }
            data.add(new Object[]{xmlPath, properties});
        }
        // Ensure that we contain the current schema version at the very least
        assertTrue("Could not find a '" + currentSchema + "' configuration file", hasCurrentSchema);
        return data;
    }

    @Override
    protected String getSubsystemXsdPath() {
        return xsdPath;
    }

    @Override
    protected String[] getSubsystemTemplatePaths() {
        return templates;
    }

    @Override
    public void testSchemaOfSubsystemTemplates() {
        // TODO: implement once the schema validator supports supplements
    }

    private KernelServices buildKernelServices() throws Exception {
        return this.buildKernelServices(this.getSubsystemXml());
    }

    private KernelServices buildKernelServices(String xml) throws Exception {
        return this.createKernelServicesBuilder(xml).build();
    }

    private KernelServicesBuilder createKernelServicesBuilder() {
        return this.createKernelServicesBuilder(AdditionalInitialization.MANAGEMENT);
    }

    private KernelServicesBuilder createKernelServicesBuilder(String xml) throws XMLStreamException {
        return this.createKernelServicesBuilder().setSubsystemXml(xml);
    }

    @Override
    protected ValidationConfiguration getModelValidationConfiguration() {
        // use this configuration to report any exceptional cases for DescriptionProviders
        return new ValidationConfiguration();
    }

    /*
     *  Create a collection of resources in the test which are not removed by a "remove" command
     *   (i.e. all resources of form /subsystem=jgroups/stack=maximal/protocol=*)
     *
     *   The list includes protocol layers used in all configuration examples.
     */
    @Override
    protected Set<PathAddress> getIgnoredChildResourcesForRemovalTest() {
        String[] protocols = { "UDP", "TCP", "MPING", "MERGE2", "FD_SOCK", "FD", "VERIFY_SUSPECT", "BARRIER",
                "pbcast.NAKACK", "pbcast.NAKACK2", "UNICAST2", "pbcast.STABLE", "pbcast.GMS", "UFC",
                "MFC", "FRAG3", "pbcast.STATE_TRANSFER", "pbcast.FLUSH",  "RSVP", "relay.RELAY2" };

        Set<PathAddress> addresses = new HashSet<>();

        PathAddress address = PathAddress.pathAddress(JGroupsSubsystemResourceDefinition.PATH);
        for (String protocol : protocols) {
            addresses.add(address.append(StackResourceDefinition.pathElement("maximal")).append(ProtocolResourceDefinition.pathElement(protocol)));
            addresses.add(address.append(ChannelResourceDefinition.pathElement("bridge")).append(ProtocolResourceDefinition.pathElement(protocol)));
            addresses.add(address.append(ChannelResourceDefinition.pathElement("ee")).append(ProtocolResourceDefinition.pathElement(protocol)));
        }

        return addresses;
    }

    /**
     * Tests that the xml is parsed into the correct operations
     */
    @Test
    public void testParseSubsystem() throws Exception {
        // Parse the subsystem xml into operations
        List<ModelNode> operations = super.parse(getSubsystemXml());

        // Check that we have the expected number of operations
        // one for each resource instance
        Assert.assertEquals(this.expectedOperationCount, operations.size());
    }

    /**
     * Starts a controller with a given subsystem xml and then checks that a second controller
     * started with the xml marshalled from the first one results in the same model
     */
    @Test
    public void testParseAndMarshalModel() throws Exception {
        KernelServices services = this.buildKernelServices();

        // Get the model and the persisted xml from the first controller
        ModelNode modelA = services.readWholeModel();
        String marshalled = services.getPersistedSubsystemXml();
        ModelNode modelB = this.buildKernelServices(marshalled).readWholeModel();

        // Make sure the models from the two controllers are identical
        super.compare(modelA, modelB);
    }

    /**
     * Starts a controller with the given subsystem xml and then checks that a second controller
     * started with the operations from its describe action results in the same model
     */
    @Test
    public void testDescribeHandler() throws Exception {
        KernelServices servicesA = this.buildKernelServices();
        // Get the model and the describe operations from the first controller
        ModelNode modelA = servicesA.readWholeModel();
        ModelNode operation = Operations.createDescribeOperation(PathAddress.pathAddress(JGroupsSubsystemResourceDefinition.PATH));
        List<ModelNode> operations = checkResultAndGetContents(servicesA.executeOperation(operation)).asList();

        // Install the describe options from the first controller into a second controller
        KernelServices servicesB = this.createKernelServicesBuilder().setBootOperations(operations).build();
        ModelNode modelB = servicesB.readWholeModel();

        // Make sure the models from the two controllers are identical
        super.compare(modelA, modelB);
    }

    @Test
    public void testLegacyOperations() throws Exception {
        List<ModelNode> ops = new LinkedList<>();
        PathAddress subsystemAddress = PathAddress.pathAddress(JGroupsSubsystemResourceDefinition.PATH);
        PathAddress udpAddress = subsystemAddress.append(StackResourceDefinition.pathElement("udp"));

        ModelNode op = Util.createAddOperation(subsystemAddress);
        ///subsystem=jgroups:add(default-stack=udp)
        op.get("default-stack").set("udp");
        ops.add(op);
        //subsystem=jgroups/stack=udp:add(transport={"type"=>"UDP","socket-binding"=>"jgroups-udp"},protocols=["PING","MERGE3","FD_SOCK","FD","VERIFY_SUSPECT","BARRIER","pbcast.NAKACK2","UNICAST2","pbcast.STABLE","pbcast.GMS","UFC","MFC","FRAG3","RSVP"])
        op = Util.createAddOperation(udpAddress);
        ModelNode transport = new ModelNode();
        transport.get("type").set("UDP");
        transport.get("socket-binding").set("jgroups-udp");

        ModelNode protocols = new ModelNode();
        String[] protocolList = {"PING", "MERGE3", "FD_SOCK", "FD", "VERIFY_SUSPECT", "BARRIER", "pbcast.NAKACK2", "UNICAST2",
                          "pbcast.STABLE", "pbcast.GMS", "UFC", "MFC", "FRAG3", "RSVP"} ;

        for (int i = 0; i < protocolList.length; i++) {
            ModelNode protocol = new ModelNode();
            protocol.get(ModelKeys.TYPE).set(protocolList[i]) ;
            protocol.get("socket-binding").set("jgroups-udp");
            protocols.add(protocol);
        }

        op.get("transport").set(transport);
        op.get("protocols").set(protocols);
        ops.add(op);
        //subsystem=jgroups/stack=udp/protocol= FD_SOCK:write-attribute(name=socket-binding,value=jgroups-udp-fd)
        //ops.add(Util.getWriteAttributeOperation(udpAddress.append(ProtocolResourceDefinition.pathElement("FD_SOCK")), "socket-binding", new ModelNode("jgroups-udp-fd")));

        KernelServices servicesA = createKernelServicesBuilder(createAdditionalInitialization()).setBootOperations(ops).build();

        Assert.assertTrue("Subsystem boot failed!", servicesA.isSuccessfulBoot());
        //Get the model and the persisted xml from the first controller
        final ModelNode modelA = servicesA.readWholeModel();
        validateModel(modelA);
        servicesA.shutdown();

        // Test the describe operation
        final ModelNode operation = createDescribeOperation();
        final ModelNode result = servicesA.executeOperation(operation);
        Assert.assertTrue("the subsystem describe operation has to generate a list of operations to recreate the subsystem",
                !result.hasDefined(ModelDescriptionConstants.FAILURE_DESCRIPTION));
        final List<ModelNode> operations = result.get(ModelDescriptionConstants.RESULT).asList();
        servicesA.shutdown();

        final KernelServices servicesC = createKernelServicesBuilder(createAdditionalInitialization()).setBootOperations(operations).build();
        final ModelNode modelC = servicesC.readWholeModel();

        compare(modelA, modelC);

        assertRemoveSubsystemResources(servicesC, getIgnoredChildResourcesForRemovalTest());
    }
}
