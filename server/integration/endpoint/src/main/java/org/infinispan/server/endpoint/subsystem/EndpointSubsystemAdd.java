/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.endpoint.subsystem;

import org.infinispan.server.endpoint.Constants;
import org.infinispan.server.endpoint.deployments.ConverterFactoryExtensionProcessor;
import org.infinispan.server.endpoint.deployments.FilterFactoryExtensionProcessor;
import org.infinispan.server.endpoint.deployments.MarshallerExtensionProcessor;
import org.infinispan.server.endpoint.deployments.ServerExtensionDependenciesProcessor;
import org.jboss.as.controller.AbstractAddStepHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.ServiceVerificationHandler;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.as.server.AbstractDeploymentChainStep;
import org.jboss.as.server.DeploymentProcessorTarget;
import org.jboss.as.server.deployment.Phase;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceBuilder;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;

import java.util.List;

import static org.infinispan.server.endpoint.subsystem.ModelKeys.*;

/**
 * @author <a href="http://gleamynode.net/">Trustin Lee</a>
 * @author Tristan Tarrant
 */
class EndpointSubsystemAdd extends AbstractAddStepHandler {

    static final EndpointSubsystemAdd INSTANCE = new EndpointSubsystemAdd();
    private static final String[] CONNECTORS = {HOTROD_CONNECTOR, MEMCACHED_CONNECTOR, REST_CONNECTOR};

    static ModelNode createOperation(ModelNode address, ModelNode existing) {
        ModelNode operation = Util.getEmptyOperation(ModelDescriptionConstants.ADD, address);
        populate(existing, operation);
        return operation;
    }

    private static void populate(ModelNode source, ModelNode target) {
        for (String connectorType : CONNECTORS) {
            target.get(connectorType).setEmptyObject();
        }
    }

    @Override
    protected void populateModel(ModelNode source, ModelNode target) throws OperationFailedException {
        populate(source, target);
    }

    @Override
    protected boolean requiresRuntimeVerification() {
        return false;
    }

    @Override
    protected void performRuntime(OperationContext ctx, ModelNode operation, ModelNode model, ServiceVerificationHandler verificationHandler, List<ServiceController<?>> newControllers) throws OperationFailedException {
        final ServiceName serviceName = Constants.EXTENSION_MANAGER_NAME;
        ExtensionManagerService service = new ExtensionManagerService();
        ServiceBuilder<?> builder = ctx.getServiceTarget().addService(serviceName, service);

        ctx.addStep(new AbstractDeploymentChainStep() {
            protected void execute(DeploymentProcessorTarget processorTarget) {
            processorTarget.addDeploymentProcessor(Constants.SUBSYSTEM_NAME,
                Phase.INSTALL, Constants.INSTALL_FILTER_FACTORY, new FilterFactoryExtensionProcessor(serviceName));
            processorTarget.addDeploymentProcessor(Constants.SUBSYSTEM_NAME,
                Phase.INSTALL, Constants.INSTALL_CONVERTER_FACTORY, new ConverterFactoryExtensionProcessor(serviceName));
            processorTarget.addDeploymentProcessor(Constants.SUBSYSTEM_NAME,
                Phase.INSTALL, Constants.INSTALL_MARSHALLER, new MarshallerExtensionProcessor(serviceName));
            processorTarget.addDeploymentProcessor(Constants.SUBSYSTEM_NAME,
                Phase.DEPENDENCIES, Constants.DEPENDENCIES, new ServerExtensionDependenciesProcessor());
            }
        }, OperationContext.Stage.RUNTIME);

        builder.install();
    }

}
