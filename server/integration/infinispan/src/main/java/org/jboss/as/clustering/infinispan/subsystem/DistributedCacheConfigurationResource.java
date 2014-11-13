package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.services.path.ResolvePathHandler;

/**
 * DistributedCacheConfigurationResource.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class DistributedCacheConfigurationResource extends DistributedCacheResource {
    public static final PathElement DISTRIBUTED_CACHE_CONFIGURATION_PATH = PathElement.pathElement(ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION);

    public DistributedCacheConfigurationResource(final ResolvePathHandler resolvePathHandler) {
        super(DISTRIBUTED_CACHE_CONFIGURATION_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.DISTRIBUTED_CACHE_CONFIGURATION),
                DistributedCacheAdd.CONFIGURATION_INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE,
                resolvePathHandler, false);
    }
}
