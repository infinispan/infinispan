package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleOperationDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.OperationEntry;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * @author Tristan Tarrant
 */
public class ClusterLoaderResource extends BaseLoaderResource {

    private static final PathElement CLUSTER_LOADER_PATH = PathElement.pathElement(ModelKeys.CLUSTER_LOADER);

    // attributes
    static final SimpleAttributeDefinition REMOTE_TIMEOUT =
            new SimpleAttributeDefinitionBuilder(ModelKeys.REMOTE_TIMEOUT, ModelType.LONG, true)
                    .setXmlName(Attribute.REMOTE_TIMEOUT.getLocalName())
                    .setAllowExpression(false)
                    .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
                    .setDefaultValue(new ModelNode().set(15000L))
                    .build();

    static final AttributeDefinition[] CLUSTER_LOADER_ATTRIBUTES = {REMOTE_TIMEOUT};

    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(BaseStoreResource.NAME)
                   .setDefaultValue(new ModelNode().set(ModelKeys.CLUSTER_LOADER_NAME))
                   .build();

    // operations
    private static final OperationDefinition CLUSTER_LOADER_ADD_DEFINITION = new SimpleOperationDefinitionBuilder(ADD, new InfinispanResourceDescriptionResolver(ModelKeys.CLUSTER_LOADER))
        .setParameters(COMMON_LOADER_PARAMETERS)
        .addParameter(REMOTE_TIMEOUT)
        .setAttributeResolver(new InfinispanResourceDescriptionResolver(ModelKeys.CLUSTER_LOADER))
        .build();

    public ClusterLoaderResource(CacheResource cacheResource) {
        super(CLUSTER_LOADER_PATH, ModelKeys.CLUSTER_LOADER, cacheResource, CLUSTER_LOADER_ATTRIBUTES);
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }

    // override the add operation to provide a custom definition (for the optional PROPERTIES parameter to add())
    @Override
    protected void registerAddOperation(final ManagementResourceRegistration registration, final OperationStepHandler handler, OperationEntry.Flag... flags) {
        registration.registerOperationHandler(CLUSTER_LOADER_ADD_DEFINITION, handler);
    }

}
