package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.as.server.ServerEnvironment;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

public class SoftIndexConfigurationResource extends BaseStoreConfigurationResource {

   static final PathElement STORE_PATH = PathElement.pathElement(ModelKeys.SOFT_INDEX_FILE_STORE);
   static final PathElement DATA_PATH = PathElement.pathElement(ModelKeys.DATA, ModelKeys.DATA_NAME);
   static final PathElement INDEX_PATH = PathElement.pathElement(ModelKeys.INDEX, ModelKeys.INDEX_NAME);

   static final SimpleAttributeDefinition NAME =
         new SimpleAttributeDefinitionBuilder(BaseStoreConfigurationResource.NAME)
               .setDefaultValue(new ModelNode().set(ModelKeys.SOFT_INDEX_FILE_STORE_NAME))
               .build();

   // SIFS Attributes
   static final SimpleAttributeDefinition COMPACTION_THRESHOLD =
         new SimpleAttributeDefinitionBuilder(ModelKeys.COMPACTION_THRESHOLD, ModelType.DOUBLE, true)
               .setXmlName(Attribute.COMPACTION_THRESHOLD.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(SoftIndexFileStoreConfiguration.COMPACTION_THRESHOLD.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition OPEN_FILES_LIMIT =
         new SimpleAttributeDefinitionBuilder(ModelKeys.OPEN_FILES_LIMIT, ModelType.INT, true)
               .setXmlName(Attribute.OPEN_FILES_LIMIT.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(SoftIndexFileStoreConfiguration.OPEN_FILES_LIMIT.getDefaultValue()))
               .build();

   // Common attributes
   static final SimpleAttributeDefinition PATH =
         new SimpleAttributeDefinitionBuilder(ModelKeys.PATH, ModelType.STRING, true)
               .setXmlName(Attribute.PATH.getLocalName())
               .setAllowExpression(false)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(ServerEnvironment.SERVER_DATA_DIR))
               .build();

   // Data attributes
   static final SimpleAttributeDefinition MAX_FILE_SIZE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_FILE_SIZE, ModelType.INT, true)
               .setXmlName(Attribute.MAX_FILE_SIZE.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(SoftIndexFileStoreConfiguration.MAX_FILE_SIZE.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition SYNC_WRITES =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SYNC_WRITES, ModelType.BOOLEAN, true)
               .setXmlName(Attribute.SYNC_WRITES.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(SoftIndexFileStoreConfiguration.SYNC_WRITES.getDefaultValue()))
               .build();

   // Index Attributes
   static final SimpleAttributeDefinition MAX_QUEUE_LENGTH =
         new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_QUEUE_LENGTH, ModelType.INT, true)
               .setXmlName(Attribute.MAX_QUEUE_LENGTH.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(SoftIndexFileStoreConfiguration.INDEX_QUEUE_LENGTH.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition SEGMENTS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.SEGMENTS, ModelType.INT, true)
               .setXmlName(Attribute.SEGMENTS.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(SoftIndexFileStoreConfiguration.INDEX_SEGMENTS.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition MAX_NODE_SIZE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.MAX_NODE_SIZE, ModelType.INT, true)
               .setXmlName(Attribute.MAX_NODE_SIZE.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(SoftIndexFileStoreConfiguration.MAX_NODE_SIZE.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition MIN_NODE_SIZE =
         new SimpleAttributeDefinitionBuilder(ModelKeys.MIN_NODE_SIZE, ModelType.INT, true)
               .setXmlName(Attribute.MIN_NODE_SIZE.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(SoftIndexFileStoreConfiguration.MIN_NODE_SIZE.getDefaultValue()))
               .build();


   static final AttributeDefinition[] ATTRIBUTES = {COMPACTION_THRESHOLD, OPEN_FILES_LIMIT};
   static final AttributeDefinition[] DATA_ATTRIBUTES = {PATH, MAX_FILE_SIZE, SYNC_WRITES};
   static final AttributeDefinition[] INDEX_ATTRIBUTES = {PATH, MAX_QUEUE_LENGTH, SEGMENTS, MAX_NODE_SIZE, MIN_NODE_SIZE};

   SoftIndexConfigurationResource(CacheConfigurationResource parent, ManagementResourceRegistration containerReg) {
      super(STORE_PATH, ModelKeys.SOFT_INDEX_FILE_STORE, parent, containerReg, ATTRIBUTES);
   }

   @Override
   public void registerChildren(ManagementResourceRegistration resourceRegistration) {
      super.registerChildren(resourceRegistration);
      resourceRegistration.registerSubModel(new DataResource(resource));
      resourceRegistration.registerSubModel(new IndexResource(resource));
   }

   static class DataResource extends CacheChildResource {
      DataResource(RestartableResourceDefinition parent) {
         super(DATA_PATH, new InfinispanResourceDescriptionResolver(ModelKeys.SOFT_INDEX_FILE_STORE, ModelKeys.DATA), parent, DATA_ATTRIBUTES);
      }
   }

   static class IndexResource extends CacheChildResource {
      IndexResource(RestartableResourceDefinition parent) {
         super(INDEX_PATH, new InfinispanResourceDescriptionResolver(ModelKeys.SOFT_INDEX_FILE_STORE, ModelKeys.INDEX), parent, INDEX_ATTRIBUTES);
      }
   }
}
