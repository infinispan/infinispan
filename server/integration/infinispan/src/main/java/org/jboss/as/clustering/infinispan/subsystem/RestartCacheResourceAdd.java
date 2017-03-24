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
import org.jboss.as.controller.RestartParentResourceAddHandler;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;

/**
 * RestartCacheResourceAdd. Restarts a cache when a child resource is added
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class RestartCacheResourceAdd extends RestartParentResourceAddHandler {
    private final AttributeDefinition[] attributes;
    private final RestartableServiceHandler parentServiceHandler;
    private final CacheServiceName serviceNameResolver;

    RestartCacheResourceAdd(String parentKeyName, RestartableServiceHandler parentServiceHandler, CacheServiceName serviceNameResolver, AttributeDefinition[] attributes) {
        super(parentKeyName);
        this.attributes = attributes;
        this.parentServiceHandler = parentServiceHandler;
        this.serviceNameResolver = serviceNameResolver;
    }

    @Override
    protected void populateModel(ModelNode operation, ModelNode model) throws OperationFailedException {
        for (AttributeDefinition attr : attributes) {
            attr.validateAndSet(operation, model);
        }
    }

    @Override
    protected void recreateParentService(OperationContext context, PathAddress resourceAddress, ModelNode resourceModel) throws OperationFailedException {
        int containerPosition = PathAddressUtils.indexOfKey(resourceAddress, CacheContainerResource.CONTAINER_PATH.getKey());
        PathAddress containerAddress = resourceAddress.subAddress(0, containerPosition + 1);
        ModelNode containerModel = context.readResourceFromRoot(containerAddress).getModel();
        ModelNode operation = Util.createAddOperation(resourceAddress);

        parentServiceHandler.installRuntimeServices(context, operation, containerModel, resourceModel);
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
    protected void removeServices(OperationContext context, ServiceName parentService, ModelNode parentModel)
            throws OperationFailedException {
        /*String containerName = parentService.getParent().getSimpleName();
        String cacheName = parentService.getSimpleName();
        ModelNode resolvedValue = null;
        String jndiName = (resolvedValue = CacheConfigurationResource.JNDI_NAME.resolveModelAttribute(context, parentModel))
                .isDefined() ? resolvedValue.asString() : null;
        ContextNames.BindInfo bindInfo;
        bindInfo = ContextNames.bindInfoFor(InfinispanJndiName.createCacheJndiName(jndiName, containerName, cacheName));
        context.removeService(bindInfo.getBinderServiceName());*/
        super.removeServices(context, parentService, parentModel);
        //context.removeService(CacheServiceName.CONFIGURATION.getServiceName(containerName, cacheName));
    }

    @Override
    protected boolean isResourceServiceRestartAllowed(OperationContext context, ServiceController<?> service) {
        return true;
    }
}
