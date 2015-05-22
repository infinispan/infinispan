package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.services.path.ResolvePathHandler;

/**
 * ReplicatedCacheConfigurationResource.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class ReplicatedCacheConfigurationResource extends ReplicatedCacheResource {
    public static final PathElement REPLICATED_CACHE_CONFIGURATION_PATH = PathElement.pathElement(ModelKeys.REPLICATED_CACHE_CONFIGURATION);

    public ReplicatedCacheConfigurationResource(final ResolvePathHandler resolvePathHandler) {
        super(REPLICATED_CACHE_CONFIGURATION_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.REPLICATED_CACHE_CONFIGURATION),
                ReplicatedCacheAdd.CONFIGURATION_INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE,
                resolvePathHandler, false);
    }

}
