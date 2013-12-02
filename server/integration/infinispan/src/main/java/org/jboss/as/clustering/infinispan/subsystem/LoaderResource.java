package org.jboss.as.clustering.infinispan.subsystem;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.ADD;

import org.jboss.as.controller.*;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.controller.registry.OperationEntry;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/loader=LOADER
 *
 * @author Tristan Tarrant
 */
public class LoaderResource extends BaseLoaderResource {

    private static final PathElement LOADER_PATH = PathElement.pathElement(ModelKeys.LOADER);

    // attributes
    static final SimpleAttributeDefinition CLASS =
            new SimpleAttributeDefinitionBuilder(ModelKeys.CLASS, ModelType.STRING, false)
                    .setXmlName(Attribute.CLASS.getLocalName())
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .build();

    static final AttributeDefinition[] LOADER_ATTRIBUTES = {CLASS};

    static final SimpleAttributeDefinition NAME =
            new SimpleAttributeDefinitionBuilder(BaseStoreResource.NAME)
                   .setDefaultValue(new ModelNode().set(ModelKeys.LOADER_NAME))
                   .build();

    // operations
    private static final OperationDefinition LOADER_ADD_DEFINITION = new SimpleOperationDefinitionBuilder(ADD, InfinispanExtension.getResourceDescriptionResolver(ModelKeys.LOADER))
        .setParameters(COMMON_LOADER_PARAMETERS)
        .addParameter(CLASS)
        .setAttributeResolver(InfinispanExtension.getResourceDescriptionResolver(ModelKeys.LOADER))
        .build();

    public LoaderResource() {
        super(LOADER_PATH,
                InfinispanExtension.getResourceDescriptionResolver(ModelKeys.LOADER),
                CacheConfigOperationHandlers.LOADER_ADD,
                ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // check that we don't need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(LOADER_ATTRIBUTES);
        for (AttributeDefinition attr : LOADER_ATTRIBUTES) {
            resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
        }
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }

    // override the add operation to provide a custom definition (for the optional PROPERTIES parameter to add())
    @Override
    protected void registerAddOperation(final ManagementResourceRegistration registration, final OperationStepHandler handler, OperationEntry.Flag... flags) {
        registration.registerOperationHandler(LOADER_ADD_DEFINITION, handler);
    }
}
