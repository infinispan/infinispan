package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.services.path.ResolvePathHandler;

/**
 * InvalidationCacheConfigurationResource.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class InvalidationCacheConfigurationResource extends InvalidationCacheResource {
    public static final PathElement INVALIDATION_CACHE_CONFIGURATION_PATH = PathElement.pathElement(ModelKeys.INVALIDATION_CACHE_CONFIGURATION);

    public InvalidationCacheConfigurationResource(final ResolvePathHandler resolvePathHandler) {
        super(INVALIDATION_CACHE_CONFIGURATION_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.INVALIDATION_CACHE_CONFIGURATION),
                InvalidationCacheAdd.CONFIGURATION_INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE,
                resolvePathHandler, false);
    }

}
