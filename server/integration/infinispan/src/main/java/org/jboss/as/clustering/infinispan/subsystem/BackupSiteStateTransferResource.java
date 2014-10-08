package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.configuration.cache.XSiteStateTransferConfigurationBuilder;
import org.jboss.as.clustering.infinispan.subsystem.CacheConfigOperationHandlers.CacheConfigAdd;
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
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Cross-Site state transfer resource.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class BackupSiteStateTransferResource extends SimpleResourceDefinition {

   static final SimpleAttributeDefinition STATE_TRANSFER_CHUNK_SIZE = new SimpleAttributeDefinitionBuilder(ModelKeys.CHUNK_SIZE, ModelType.INT, true)
         .setXmlName(Attribute.CHUNK_SIZE.getLocalName())
         .setAllowExpression(true)
         .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
         .setDefaultValue(new ModelNode().set(XSiteStateTransferConfigurationBuilder.DEFAULT_CHUNK_SIZE))
         .build();
   static final SimpleAttributeDefinition STATE_TRANSFER_TIMEOUT = new SimpleAttributeDefinitionBuilder(ModelKeys.TIMEOUT, ModelType.LONG, true)
         .setXmlName(Attribute.TIMEOUT.getLocalName())
         .setAllowExpression(true)
         .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
         .setDefaultValue(new ModelNode().set(XSiteStateTransferConfigurationBuilder.DEFAULT_TIMEOUT))
         .build();
   static final SimpleAttributeDefinition STATE_TRANSFER_WAIT_TIME = new SimpleAttributeDefinitionBuilder(ModelKeys.WAIT_TIME, ModelType.LONG, true)
         .setXmlName(Attribute.WAIT_TIME.getLocalName())
         .setAllowExpression(true)
         .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
         .setDefaultValue(new ModelNode().set(XSiteStateTransferConfigurationBuilder.DEFAULT_WAIT_TIME))
         .build();
   static final SimpleAttributeDefinition STATE_TRANSFER_MAX_RETRIES = new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_RETRIES, ModelType.INT, true)
         .setXmlName(Attribute.MAX_RETRIES.getLocalName())
         .setAllowExpression(true)
         .setFlags(AttributeAccess.Flag.RESTART_ALL_SERVICES)
         .setDefaultValue(new ModelNode().set(XSiteStateTransferConfigurationBuilder.DEFAULT_MAX_RETRIES))
         .build();

   static final AttributeDefinition[] ATTRIBUTES = new AttributeDefinition[]{STATE_TRANSFER_CHUNK_SIZE, STATE_TRANSFER_MAX_RETRIES, STATE_TRANSFER_TIMEOUT, STATE_TRANSFER_WAIT_TIME};

   private final boolean runtimeRegistration;

   BackupSiteStateTransferResource(boolean runtimeRegistration) {
      super(PathElement.pathElement(ModelKeys.STATE_TRANSFER, ModelKeys.STATE_TRANSFER_NAME), InfinispanExtension.getResourceDescriptionResolver(ModelKeys.BACKUP, ModelKeys.STATE_TRANSFER), new CacheConfigAdd(ATTRIBUTES), ReloadRequiredRemoveStepHandler.INSTANCE);
      this.runtimeRegistration = runtimeRegistration;
   }

   @Override
   public void registerAttributes(ManagementResourceRegistration registration) {
      final OperationStepHandler writeHandler = new ReloadRequiredWriteAttributeHandler(ATTRIBUTES);
      for (AttributeDefinition attribute : ATTRIBUTES) {
         registration.registerReadWriteAttribute(attribute, null, writeHandler);
      }
   }

   @Override
   public void registerOperations(ManagementResourceRegistration resourceRegistration) {
      super.registerOperations(resourceRegistration);
   }

   public boolean isRuntimeRegistration() {
      return runtimeRegistration;
   }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      super.registerChildren(resourceRegistration);
   }
}
