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
package org.jboss.as.clustering.infinispan.subsystem;

import java.io.FileReader;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.infinispan.server.commons.controller.Operations;
import org.infinispan.server.commons.subsystem.ClusteringSubsystemTest;
import org.infinispan.server.jgroups.subsystem.JGroupsSubsystemResourceDefinition;
import org.jboss.as.controller.PathAddress;
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
 * Tests parsing / booting / marshalling of Infinispan configurations.
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
        super(InfinispanExtension.SUBSYSTEM_NAME, new InfinispanExtension(), xmlPath.getFileName().toString());
        this.expectedOperationCount = Integer.parseInt(properties.getProperty("expected.operations.count"));
        this.xsdPath = properties.getProperty("xsd.path");
        this.templates = null;
    }

    @Parameters
    public static Collection<Object[]> data() throws Exception {
        URL configDir = Thread.currentThread().getContextClassLoader().getResource("org/jboss/as/clustering/infinispan/subsystem");
        List<Path> paths = Files.list(Paths.get(configDir.toURI()))
              .filter(path -> path.getFileName().toString().matches("^subsystem-infinispan_[0-9]+_[0-9]+.xml$"))
              .collect(Collectors.toList());

        List<Object[]> data = new ArrayList<>();
        for (int i = 0; i < paths.size(); i++) {
            Path xmlPath = paths.get(i);
            String propsPath = xmlPath.toString().replaceAll("\\.xml$", ".properties");
            Properties properties = new Properties();
            try (Reader r = new FileReader(propsPath)) {
                properties.load(r);
            }
            data.add(new Object[]{xmlPath, properties});
        }
        return data;
    }

    @Override
    protected String getSubsystemXsdPath() {
        return xsdPath;
    }

    @Override
    protected Properties getResolvedProperties() {
        Properties properties = new Properties();
        properties.setProperty("java.io.tmpdir", System.getProperty("java.io.tmpdir"));

        return properties;
    }

    @Override
    public void testSchemaOfSubsystemTemplates() {
        // TODO: implement once the schema validator supports supplements
    }

    @Override
    protected AdditionalInitialization createAdditionalInitialization() {
        return new org.infinispan.server.jgroups.subsystem.JGroupsSubsystemInitialization();
    }

    @Override
    protected void compare(ModelNode model1, ModelNode model2) {
        purgeJGroupsModel(model1);
        purgeJGroupsModel(model2);
        super.compare(model1, model2);
    }

    private static void purgeJGroupsModel(ModelNode model) {
        model.get(JGroupsSubsystemResourceDefinition.PATH.getKey()).remove(JGroupsSubsystemResourceDefinition.PATH.getValue());
    }

    @Override
    protected ValidationConfiguration getModelValidationConfiguration() {
        // use this configuration to report any exceptional cases for DescriptionProviders
        return new ValidationConfiguration();
    }

    /**
     * Tests that the xml is parsed into the correct operations
     */
    @Test
    public void testParseSubsystem() throws Exception {
        // Parse the subsystem xml into operations
        List<ModelNode> operations = this.parse(this.createAdditionalInitialization(), getSubsystemXml());

        // Check that we have the expected number of operations
        // one for each resource instance
        Assert.assertEquals(operations.toString(), this.expectedOperationCount, operations.size());
    }

    /**
     * Test that the model created from the xml looks as expected
     */
    @Test
    public void testInstallIntoController() throws Exception {
        // Parse the subsystem xml and install into the controller
        KernelServices services = createKernelServicesBuilder().setSubsystemXml(getSubsystemXml()).build();

        // Read the whole model and make sure it looks as expected
        ModelNode model = services.readWholeModel();

        Assert.assertTrue(model.get(InfinispanSubsystemRootResource.PATH.getKey()).hasDefined(InfinispanSubsystemRootResource.PATH.getValue()));
    }

    private KernelServicesBuilder createKernelServicesBuilder() {
        return this.createKernelServicesBuilder(this.createAdditionalInitialization());
    }

    /**
     * Starts a controller with a given subsystem xml and then checks that a second controller
     * started with the xml marshalled from the first one results in the same model
     */
    @Test
    public void testParseAndMarshalModel() throws Exception {
        // Parse the subsystem xml and install into the first controller

        KernelServices servicesA = this.createKernelServicesBuilder().setSubsystemXml(this.getSubsystemXml()).build();

        // Get the model and the persisted xml from the first controller
        ModelNode modelA = servicesA.readWholeModel();

        String marshalled = servicesA.getPersistedSubsystemXml();

        // Install the persisted xml from the first controller into a second controller
        KernelServices servicesB = this.createKernelServicesBuilder().setSubsystemXml(marshalled).build();
        ModelNode modelB = servicesB.readWholeModel();

        // Make sure the models from the two controllers are identical
        this.compare(modelA, modelB);
    }

    /**
     * Starts a controller with the given subsystem xml and then checks that a second controller
     * started with the operations from its describe action results in the same model
     */
    @Test
    public void testDescribeHandler() throws Exception {
        // Parse the subsystem xml and install into the first controller
        KernelServices servicesA = this.createKernelServicesBuilder().setSubsystemXml(this.getSubsystemXml()).build();
        // Get the model and the describe operations from the first controller
        ModelNode modelA = servicesA.readWholeModel();

        ModelNode operation = Operations.createDescribeOperation(PathAddress.pathAddress(InfinispanSubsystemRootResource.PATH));
        List<ModelNode> operations = checkResultAndGetContents(servicesA.executeOperation(operation)).asList();

        // Install the describe options from the first controller into a second controller
        KernelServices servicesB = this.createKernelServicesBuilder().setBootOperations(operations).build();
        ModelNode modelB = servicesB.readWholeModel();

        // Make sure the models from the two controllers are identical
        this.compare(modelA, modelB);
    }
}
