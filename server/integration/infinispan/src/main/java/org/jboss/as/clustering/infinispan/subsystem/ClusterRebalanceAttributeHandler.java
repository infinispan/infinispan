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

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.infinispan.SecurityActions;
import org.infinispan.server.infinispan.spi.service.CacheContainerServiceName;
import org.infinispan.topology.ClusterTopologyManager;
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
public class ClusterRebalanceAttributeHandler extends AbstractRuntimeOnlyHandler {

    public static final ClusterRebalanceAttributeHandler INSTANCE = new ClusterRebalanceAttributeHandler();

    @Override
    public void executeRuntimeStep(OperationContext context, ModelNode operation) throws OperationFailedException {
        final PathAddress address = PathAddress.pathAddress(operation.require(OP_ADDR));
        final String cacheContainerName = address.getElement(address.size() - 1).getValue();
        final ServiceController<?> controller = context.getServiceRegistry(false).getService(
              CacheContainerServiceName.CACHE_CONTAINER.getServiceName(cacheContainerName));
        if (controller != null) {
            final EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) controller.getValue();
            if (cacheContainerName != null) {
                ClusterTopologyManager topologyManager = SecurityActions.getGlobalComponentRegistry(cacheManager)
                      .getComponent(ClusterTopologyManager.class);
                if (topologyManager != null) {
                    try {
                        if (operation.hasDefined(VALUE)) {
                            ModelNode newValue = operation.get(VALUE);
                            topologyManager.setRebalancingEnabled(newValue.asBoolean());
                        } else {
                            context.getResult().set(new ModelNode().set(topologyManager.isRebalancingEnabled()));
                        }
                    } catch (Exception e) {
                        throw new OperationFailedException(MESSAGES.failedToInvokeOperation(e.getLocalizedMessage()));
                    }
                }
            }
        }
        context.stepCompleted();
    }
}
