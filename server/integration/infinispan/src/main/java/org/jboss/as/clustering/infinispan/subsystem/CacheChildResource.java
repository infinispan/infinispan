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
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.descriptions.ResourceDescriptionResolver;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

/**
 * CacheChildResource.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
abstract class CacheChildResource extends SimpleResourceDefinition {
    protected final RestartableResourceDefinition resource;
    protected final AttributeDefinition[] attributes;

    CacheChildResource(PathElement path, String resourceKey, RestartableResourceDefinition resource) {
        this(path, resourceKey, resource, new AttributeDefinition[]{});
    }

    CacheChildResource(PathElement path, String resourceKey, RestartableResourceDefinition resource,
            AttributeDefinition[] attributes) {
        super(path, new InfinispanResourceDescriptionResolver(resourceKey),
                new RestartCacheResourceAdd(resource.getPathElement().getKey(), resource.getServiceInstaller(), CacheServiceName.CACHE, attributes),
                new RestartCacheResourceRemove(resource.getPathElement().getKey(), resource.getServiceInstaller()));
        this.resource = resource;
        this.attributes = attributes;
    }

    CacheChildResource(PathElement path, ResourceDescriptionResolver resolver, RestartableResourceDefinition resource,
                       AttributeDefinition[] attributes) {
        super(path, resolver,
              new RestartCacheResourceAdd(resource.getPathElement().getKey(), resource.getServiceInstaller(), CacheServiceName.CACHE, attributes),
              new RestartCacheResourceRemove(resource.getPathElement().getKey(), resource.getServiceInstaller()));
        this.resource = resource;
        this.attributes = attributes;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        if (attributes != null) {
            final OperationStepHandler restartCacheWriteHandler = new RestartServiceWriteAttributeHandler(resource
                    .getPathElement().getKey(), resource.getServiceInstaller(), CacheServiceName.CACHE, attributes);
            for (AttributeDefinition attr : attributes) {
                resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, restartCacheWriteHandler);
            }
        }
    }
}
