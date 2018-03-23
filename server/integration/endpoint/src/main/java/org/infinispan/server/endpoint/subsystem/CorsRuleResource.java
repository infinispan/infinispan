package org.infinispan.server.endpoint.subsystem;

import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.ListAttributeDefinition;
import org.jboss.as.controller.OperationStepHandler;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.ReloadRequiredAddStepHandler;
import org.jboss.as.controller.ReloadRequiredRemoveStepHandler;
import org.jboss.as.controller.ReloadRequiredWriteAttributeHandler;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.SimpleResourceDefinition;
import org.jboss.as.controller.StringListAttributeDefinition;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * @since 9.2
 */
public class CorsRuleResource extends SimpleResourceDefinition {
   private static final PathElement CORS_RULE_PATH = PathElement.pathElement(ModelKeys.CORS_RULE);

   static final SimpleAttributeDefinition NAME =
         new SimpleAttributeDefinitionBuilder(ModelKeys.NAME, ModelType.STRING)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.NAME)
               .build();

   static final SimpleAttributeDefinition ALLOW_CREDENTIALS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.ALLOW_CREDENTIALS, ModelType.BOOLEAN, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.ALLOW_CREDENTIALS)
               .setDefaultValue(new ModelNode().set(true))
               .build();

   static final SimpleAttributeDefinition MAX_AGE_SECONDS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_AGE_SECONDS, ModelType.INT, true)
               .setAllowExpression(true)
               .setXmlName(ModelKeys.MAX_AGE_SECONDS)
               .setDefaultValue(new ModelNode().set(0))
               .build();

   static final ListAttributeDefinition ALLOWED_HEADERS =
         new StringListAttributeDefinition.Builder(ModelKeys.ALLOWED_HEADERS)
               .setRequired(false)
               .build();

   static final ListAttributeDefinition ALLOWED_METHODS =
         new StringListAttributeDefinition.Builder(ModelKeys.ALLOWED_METHODS)
               .setRequired(false)
               .build();

   static final ListAttributeDefinition ALLOWED_ORIGINS =
         new StringListAttributeDefinition.Builder(ModelKeys.ALLOWED_ORIGINS)
               .setRequired(false)
               .build();

   static final ListAttributeDefinition EXPOSE_HEADERS =
         new StringListAttributeDefinition.Builder(ModelKeys.EXPOSE_HEADERS)
               .setRequired(false)
               .build();

   static final AttributeDefinition[] CORS_RULE_ATTRIBUTES = {NAME, ALLOW_CREDENTIALS, MAX_AGE_SECONDS, ALLOWED_HEADERS,
         ALLOWED_METHODS, ALLOWED_ORIGINS, EXPOSE_HEADERS};

   CorsRuleResource() {
      super(CORS_RULE_PATH, EndpointExtension.getResourceDescriptionResolver(ModelKeys.CORS_RULE),
            new ReloadRequiredAddStepHandler(CORS_RULE_ATTRIBUTES), ReloadRequiredRemoveStepHandler.INSTANCE);
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration resourceRegistration) {
      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(CORS_RULE_ATTRIBUTES);
      for (AttributeDefinition attr : CORS_RULE_ATTRIBUTES) {
         resourceRegistration.registerReadWriteAttribute(attr, null, writeHandler);
      }
   }

}
