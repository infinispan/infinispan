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
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.RestartParentResourceRemoveHandler;
import org.jboss.as.controller.operations.common.Util;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;

/**
 * RestartCacheResourceRemove. Restarts the cache when a child resource has been removed.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class RestartCacheResourceRemove extends RestartParentResourceRemoveHandler {
    private final RestartableServiceHandler parentServiceHandler;

    protected RestartCacheResourceRemove(String parentKeyName, RestartableServiceHandler parentServiceHandler) {
        super(parentKeyName);
        this.parentServiceHandler = parentServiceHandler;
    }

    @Override
    protected void recreateParentService(OperationContext context, PathAddress cacheAddress, ModelNode cacheModel) throws OperationFailedException {
        PathAddress containerAddress = cacheAddress.subAddress(0, cacheAddress.size() - 1);
        ModelNode containerModel = context.readResourceFromRoot(containerAddress).getModel();
        ModelNode operation = Util.createAddOperation(cacheAddress);

        parentServiceHandler.installRuntimeServices(context, operation, containerModel, cacheModel);
    }

    @Override
    protected ServiceName getParentServiceName(PathAddress parentAddress) {
        int position = parentAddress.size();
        PathAddress cacheAddress = parentAddress.subAddress(position - 1);
        PathAddress containerAddress = parentAddress.subAddress(position - 2, position - 1);
        return CacheServiceName.CACHE.getServiceName(containerAddress.getLastElement().getValue(), cacheAddress.getLastElement()
                .getValue());
    }

    @Override
    protected boolean isResourceServiceRestartAllowed(OperationContext context, ServiceController<?> service) {
        return true;
    }
}
