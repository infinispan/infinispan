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
    protected final RestartableResourceDefinition resource;
    protected final AttributeDefinition[] attributes;

    CacheChildResource(PathElement path, String resourceKey, RestartableResourceDefinition resource) {
        this(path, resourceKey, resource, new AttributeDefinition[]{});
    }

    CacheChildResource(PathElement path, String resourceKey, RestartableResourceDefinition resource,
            AttributeDefinition[] attributes) {
        super(path, new InfinispanResourceDescriptionResolver(resourceKey),
                new RestartCacheResourceAdd(resource.getPathElement().getKey(), resource.getServiceInstaller(), attributes),
                new RestartCacheResourceRemove(resource.getPathElement().getKey(), resource.getServiceInstaller()));
        this.resource = resource;
        this.attributes = attributes;
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        if (attributes != null) {
            final OperationStepHandler restartCacheWriteHandler = new RestartCacheWriteAttributeHandler(resource
                    .getPathElement().getKey(), resource.getServiceInstaller(), attributes);
            for (AttributeDefinition attr : attributes) {
                resourceRegistration.registerReadWriteAttribute(attr, CacheReadAttributeHandler.INSTANCE, restartCacheWriteHandler);
            }
        }
    }
}
