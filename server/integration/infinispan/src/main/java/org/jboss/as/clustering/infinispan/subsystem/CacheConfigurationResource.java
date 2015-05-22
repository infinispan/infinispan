package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.services.path.ResolvePathHandler;

/**
 * CacheConfigurationResource.
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public class CacheConfigurationResource extends SimpleResourceDefinition {
    protected final ResolvePathHandler resolvePathHandler;
    protected final boolean runtimeRegistration;

    public static final PathElement CONFIGURATIONS_PATH = PathElement.pathElement(ModelKeys.CONFIGURATIONS, ModelKeys.CONFIGURATIONS_NAME);

    public CacheConfigurationResource(ResolvePathHandler resolvePathHandler, boolean runtimeRegistration) {
        super(CONFIGURATIONS_PATH, InfinispanExtension.getResourceDescriptionResolver(ModelKeys.CONFIGURATIONS),
                CacheConfigOperationHandlers.CONFIGURATIONS_ADD, ReloadRequiredRemoveStepHandler.INSTANCE);
        this.resolvePathHandler = resolvePathHandler;
        this.runtimeRegistration = runtimeRegistration;
    }

    @Override
    public void registerChildren(ManagementResourceRegistration resourceRegistration) {
        super.registerChildren(resourceRegistration);
        resourceRegistration.registerSubModel(new LocalCacheConfigurationResource(resolvePathHandler));
        resourceRegistration.registerSubModel(new InvalidationCacheConfigurationResource(resolvePathHandler));
        resourceRegistration.registerSubModel(new ReplicatedCacheConfigurationResource(resolvePathHandler));
        resourceRegistration.registerSubModel(new DistributedCacheConfigurationResource(resolvePathHandler));
    }
}
