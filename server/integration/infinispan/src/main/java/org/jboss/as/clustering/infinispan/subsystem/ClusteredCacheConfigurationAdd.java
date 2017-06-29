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

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationContext;
import org.jboss.as.controller.OperationFailedException;
import org.jboss.dmr.ModelNode;

/**
 * Base class for clustered cache configuration add operations
 *
 * @author Richard Achmatowicz (c) 2011 Red Hat Inc.
 */
public abstract class ClusteredCacheConfigurationAdd extends CacheConfigurationAdd {

    ClusteredCacheConfigurationAdd(CacheMode mode) {
        super(mode);
    }

    @Override
    void populate(ModelNode fromModel, ModelNode toModel) throws OperationFailedException {
        super.populate(fromModel, toModel);

        for (AttributeDefinition attribute : ClusteredCacheConfigurationResource.ATTRIBUTES) {
            attribute.validateAndSet(fromModel, toModel);
        }
    }

    /**
     * Create a Configuration object initialized from the data in the operation.
     *
     * @param cache data representing cache configuration
     * @param builder
     * @param dependencies
     *
     * @return initialised Configuration object
     */
    @Override
    void processModelNode(OperationContext context, String containerName, ModelNode cache, ConfigurationBuilder builder, List<Dependency<?>> dependencies)
            throws OperationFailedException {

        // process cache attributes and elements
        super.processModelNode(context, containerName, cache, builder, dependencies);

        // adjust the cache mode used based on the value of clustered attribute MODE
        ModelNode modeModel = ClusteredCacheConfigurationResource.MODE.resolveModelAttribute(context, cache);
        CacheMode cacheMode = modeModel.isDefined() ? Mode.valueOf(modeModel.asString()).apply(this.mode) : this.mode;
        builder.clustering().cacheMode(cacheMode);

        final long remoteTimeout = ClusteredCacheConfigurationResource.REMOTE_TIMEOUT.resolveModelAttribute(context, cache).asLong();
        // process clustered cache attributes and elements
        if (cacheMode.isSynchronous()) {
            builder.clustering().remoteTimeout(remoteTimeout);
        }
    }
}
