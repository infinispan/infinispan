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

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.server.infinispan.spi.service.CacheServiceName;
import org.jboss.as.controller.AbstractWriteAttributeHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.descriptions.ModelDescriptionConstants;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.msc.service.ServiceRegistry;

/**
 * RuntimeWriteAttributeHandler.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public class RuntimeCacheConfigurationWriteAttributeHandler extends AbstractWriteAttributeHandler<Void> {

    private final RuntimeCacheConfigurationApplier applier;
    private final SimpleAttributeDefinition attributeDef;

    public RuntimeCacheConfigurationWriteAttributeHandler(SimpleAttributeDefinition attributeDef, RuntimeCacheConfigurationApplier applier) {
        super(attributeDef);
        this.attributeDef = attributeDef;
        this.applier = applier;
    }

    @Override
    protected boolean applyUpdateToRuntime(OperationContext context, ModelNode operation, String attributeName,
            ModelNode newValue, ModelNode currentValue, HandbackHolder<Void> handbackHolder)
                    throws OperationFailedException {

        final ModelNode model = context.readResource(PathAddress.EMPTY_ADDRESS).getModel();
        applyModelToRuntime(context, operation, attributeName, model);

        return false;
    }

    private void applyModelToRuntime(OperationContext context, ModelNode operation, String attributeName,
            ModelNode model) throws OperationFailedException {
        final PathAddress address = PathAddress.pathAddress(operation.get(ModelDescriptionConstants.OP_ADDR));
        final ServiceName serviceName = getParentServiceName(address);
        final ServiceRegistry registry = context.getServiceRegistry(true);
        ServiceController<?> sc = registry.getService(serviceName);
        if (sc != null) {
            CacheConfigurationService ccs = (CacheConfigurationService) sc.getService();
            Configuration configuration = ccs.getValue();
            ModelNode value = attributeDef.resolveModelAttribute(context, model);
            applier.applyConfiguration(configuration, value);
        }
    }

    @Override
    protected void revertUpdateToRuntime(OperationContext context, ModelNode operation, String attributeName,
            ModelNode valueToRestore, ModelNode valueToRevert, Void handback) throws OperationFailedException {
        final ModelNode restored = context.readResource(PathAddress.EMPTY_ADDRESS).getModel().clone();
        restored.get(attributeName).set(valueToRestore);
        applyModelToRuntime(context, operation, attributeName, restored);
    }

    protected ServiceName getParentServiceName(PathAddress parentAddress) {
        int containerIndex = PathAddressUtils.indexOf(parentAddress, CacheContainerResource.CONTAINER_PATH);
        String containerName = parentAddress.getElement(containerIndex).getValue();
        String configurationName = parentAddress.getElement(containerIndex + 2).getValue();
        return CacheServiceName.CONFIGURATION.getServiceName(containerName, configurationName);
    }
}
