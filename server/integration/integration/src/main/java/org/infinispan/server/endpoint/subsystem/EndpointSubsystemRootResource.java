/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.SUBSYSTEM;

import org.infinispan.server.endpoint.Constants;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.operations.common.GenericSubsystemDescribeHandler;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

/**
 * The root resource of the Endpoint subsystem.
 *
 * @author Tristan Tarrant
 */
public class EndpointSubsystemRootResource extends SimpleResourceDefinition {

    private final boolean runtimeRegistration;

    public EndpointSubsystemRootResource(boolean runtimeRegistration) {
        super(PathElement.pathElement(SUBSYSTEM, Constants.SUBSYSTEM_NAME),
                EndpointExtension.getResourceDescriptionResolver(Constants.SUBSYSTEM_NAME),
                EndpointSubsystemAdd.INSTANCE,
                ReloadRequiredRemoveStepHandler.INSTANCE);
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
        resourceRegistration.registerOperationHandler(GenericSubsystemDescribeHandler.DEFINITION, GenericSubsystemDescribeHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        resourceRegistration.registerSubModel(new HotRodConnectorResource(isRuntimeRegistration()));
        resourceRegistration.registerSubModel(new MemcachedConnectorResource(isRuntimeRegistration()));
        resourceRegistration.registerSubModel(new RestConnectorResource(isRuntimeRegistration()));
        resourceRegistration.registerSubModel(new WebSocketConnectorResource(isRuntimeRegistration()));
    }

    public boolean isRuntimeRegistration() {
        return runtimeRegistration;
    }
}
