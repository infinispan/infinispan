package org.jboss.as.clustering.infinispan.subsystem;

import org.jboss.as.clustering.infinispan.subsystem.CacheConfigOperationHandlers.CacheConfigAdd;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelType;

/**
 * AuthorizationRoleResource.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationRoleResource extends SimpleResourceDefinition {

    static final SimpleAttributeDefinition NAME = new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING, true)
        .setXmlName(Attribute.NAME.getLocalName())
        .setAllowExpression(true)
        .setAllowNull(false)
        .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
        .build()
    ;

    static final StringListAttributeDefinition PERMISSIONS = new StringListAttributeDefinition.Builder(ModelKeys.PERMISSIONS)
        .build()
    ;

    static final AttributeDefinition[] ATTRIBUTES = new AttributeDefinition[] { NAME, PERMISSIONS };

    AuthorizationRoleResource() {
        super(PathElement.pathElement(ModelKeys.ROLE), InfinispanExtension.getResourceDescriptionResolver(ModelKeys.CACHE_CONTAINER, ModelKeys.SECURITY, ModelKeys.AUTHORIZATION, ModelKeys.ROLE), new CacheConfigAdd(ATTRIBUTES), ReloadRequiredRemoveStepHandler.INSTANCE);
    }

    @Override
    public void registerAttributes(ManagementResourceRegistration registration) {
        final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ATTRIBUTES);
        for (AttributeDefinition attribute: ATTRIBUTES) {
            registration.registerReadWriteAttribute(attribute, null, writeHandler);
        }
    }

}
