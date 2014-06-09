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
package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.clustering.infinispan.InfinispanMessages.MESSAGES;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.OP_ADDR;
import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.VALUE;

import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.LocalTopologyManagerImpl;
import org.jboss.as.controller.AbstractRuntimeOnlyHandler;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.PathAddress;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.ServiceController;

/**
 * RebalancingAttributeHandler.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class RebalancingAttributeHandler extends AbstractRuntimeOnlyHandler {

    public static final RebalancingAttributeHandler INSTANCE = new RebalancingAttributeHandler();

    @Override
    public void executeRuntimeStep(OperationContext context, ModelNode operation) throws OperationFailedException {
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getElement(address.size() - 2).getValue();
        final String cacheName = address.getElement(address.size() - 1).getValue();
        final ServiceController<?> controller = context.getServiceRegistry(false).getService(
                CacheService.getServiceName(cacheContainerName, cacheName));
        if (controller != null) {
            Cache<?, ?> cache = (Cache<?, ?>) controller.getValue();
            if (cache != null) {
                ComponentRegistry registry = SecurityActions.getComponentRegistry(cache.getAdvancedCache());
                LocalTopologyManagerImpl localTopologyManager = (LocalTopologyManagerImpl) registry
                      .getGlobalComponentRegistry().getComponent(LocalTopologyManager.class);
                if (localTopologyManager != null) {
                    try {
                        if (operation.hasDefined(VALUE)) {
                            ModelNode newValue = operation.get(VALUE);
                            localTopologyManager.setRebalancingEnabled(newValue.asBoolean());
                        } else {
                            context.getResult().set(new ModelNode().set(localTopologyManager.isRebalancingEnabled()));
                        }
                    } catch (Exception e) {
                        throw new OperationFailedException(new ModelNode().set(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage())));
                    }
                }
            }
        }
        context.stepCompleted();
    }
}
