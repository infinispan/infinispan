package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;

/**
 * CacheChildResource.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
abstract class CacheChildResource extends SimpleResourceDefinition {
    protected final CacheResource cacheResource;
    protected final AttributeDefinition[] attributes;

    CacheChildResource(PathElement path, String resourceKey, CacheResource cacheResource) {
        this(path, resourceKey, cacheResource, new AttributeDefinition[]{});
    }

    CacheChildResource(PathElement path, String resourceKey, CacheResource cacheResource,
            AttributeDefinition[] attributes) {
        super(path, new InfinispanResourceDescriptionResolver(resourceKey),
                new RestartCacheResourceAdd(cacheResource.getPathElement().getKey(), cacheResource.getCacheAddHandler(), attributes),
                new RestartCacheResourceRemove(cacheResource.getPathElement().getKey(), cacheResource.getCacheAddHandler()));
        this.cacheResource = cacheResource;
        this.attributes = attributes;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        if (attributes != null) {
            final OperationStepHandler restartCacheWriteHandler = new RestartCacheWriteAttributeHandler(cacheResource
                    .getPathElement().getKey(), cacheResource.getCacheAddHandler(), attributes);
            for (AttributeDefinition attr : attributes) {
                resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, restartCacheWriteHandler);
            }
        }
    }
}
