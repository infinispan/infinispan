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

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/distributed-cache=*
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 * @author Radoslav Husar
 */
public class DistributedCacheResource extends SharedCacheResource {

    public static final PathElement PATH = PathElement.pathElement(ModelKeys.DISTRIBUTED_CACHE);

    // attributes
    static final SimpleAttributeDefinition REBALANCING =
            new SimpleAttributeDefinitionBuilder(ModelKeys.REBALANCING, ModelType.BOOLEAN, true)
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_NONE)
                    .setDefaultValue(new ModelNode().set(true))
                    .setStorageRuntime()
                    .build();

    public DistributedCacheResource(final ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(PATH,
                new InfinispanResourceDescriptionResolver(ModelKeys.DISTRIBUTED_CACHE),
                DistributedCacheAdd.INSTANCE,
                new CacheRemoveHandler(), resolvePathHandler, runtimeRegistration);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        if (runtimeRegistration) {
            // register writable attributes available only at runtime
            resourceRegistration.registerReadWriteAttribute(REBALANCING, RebalancingAttributeHandler.INSTANCE, RebalancingAttributeHandler.INSTANCE);
        }
    }
}
