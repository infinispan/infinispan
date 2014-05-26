package org.infinispan.server.endpoint.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * @author Tristan Tarrant
 */
public class SaslPropertyResource extends SimpleResourceDefinition {

    static final PathElement SASL_PROPERTY_PATH = PathElement.pathElement(ModelKeys.PROPERTY);

    // attributes
    static final SimpleAttributeDefinition VALUE =
            new SimpleAttributeDefinitionBuilder("value", ModelType.STRING, false)
                    .setXmlName("value")
                    .setAllowExpression(true)
                    .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
                    .build();

    public SaslPropertyResource() {
        super(SASL_PROPERTY_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.PROPERTY), SaslPropertyAdd.INSTANCE, ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    static final AttributeDefinition[] SASL_PROPERTY_ATTRIBUTES = {VALUE};

    @Override
    public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
        super.registerAttributes(resourceRegistration);

        // do we need a special handler here?
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(VALUE);
        resourceRegistration.registerReadWriteAttribute(VALUE, null, writeHandler);
    }

    @Override
    public void registerOperations(ManagementResourceRegistration resourceRegistration) {
        super.registerOperations(resourceRegistration);
    }
}
