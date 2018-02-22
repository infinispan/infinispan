/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012, Red Hat, Inc., and individual contributors
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

import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.RestartParentWriteAttributeHandler;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;

public class RestartServiceWriteAttributeHandler extends RestartParentWriteAttributeHandler {
    private final RestartableServiceHandler serviceInstaller;
    private final CacheServiceName serviceNameResolver;

    public RestartServiceWriteAttributeHandler(String parentKeyName, RestartableServiceHandler serviceInstaller, CacheServiceName serviceNameResolver, AttributeDefinition... definitions) {
        super(parentKeyName, definitions);
        this.serviceInstaller = serviceInstaller;
        this.serviceNameResolver = serviceNameResolver;
    }

    @Override
    protected void recreateParentService(OperationContext context, PathAddress serviceAddress, ModelNode model) throws OperationFailedException {
        int containerPosition = PathAddressUtils.indexOfKey(serviceAddress, CacheContainerResource.CONTAINER_PATH.getKey());
        PathAddress containerAddress = serviceAddress.subAddress(0, containerPosition + 1) ;
        ModelNode containerModel = context.readResourceFromRoot(containerAddress).getModel();
        ModelNode operation = Util.createAddOperation(serviceAddress);
        serviceInstaller.installRuntimeServices(context, operation, containerModel, model);
    }

    @Override
    protected ServiceName getParentServiceName(PathAddress parentAddress) {
        int containerPosition = PathAddressUtils.indexOfKey(parentAddress, CacheContainerResource.CONTAINER_PATH.getKey());
        String containerName = parentAddress.getElement(containerPosition).getValue();
        int resourceOffset = containerPosition + 1;
        if (parentAddress.getElement(resourceOffset).equals(CacheContainerConfigurationsResource.PATH)) {
            resourceOffset++;
        }
        String resourceName = parentAddress.getElement(resourceOffset).getValue();
        return serviceNameResolver.getServiceName(containerName, resourceName);
    }

    @Override
    protected boolean isResourceServiceRestartAllowed(OperationContext context, ServiceController<?> service) {
        return true;
    }
}
