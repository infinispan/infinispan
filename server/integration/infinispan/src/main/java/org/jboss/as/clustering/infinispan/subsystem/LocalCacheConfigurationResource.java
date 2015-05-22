package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.services.path.ResolvePathHandler;

/**
 * LocalCacheConfigurationResource.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class LocalCacheConfigurationResource extends LocalCacheResource {
    public static final PathElement LOCAL_CACHE_CONFIGURATION_PATH = PathElement.pathElement(ModelKeys.LOCAL_CACHE_CONFIGURATION);

    public LocalCacheConfigurationResource(final ResolvePathHandler resolvePathHandler) {
        super(LOCAL_CACHE_CONFIGURATION_PATH, InfinispanExtension
                .getResourceDescriptionResolver(ModelKeys.LOCAL_CACHE_CONFIGURATION),
                LocalCacheAdd.CONFIGURATION_INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE,
                resolvePathHandler, false);
    }

}
