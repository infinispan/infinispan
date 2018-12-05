package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.jboss.as.controller.AttributeDefinition;
import org.jboss.as.controller.PathElement;
import org.jboss.as.controller.SimpleAttributeDefinition;
import org.jboss.as.controller.SimpleAttributeDefinitionBuilder;
import org.jboss.as.controller.registry.AttributeAccess;
import org.jboss.as.controller.registry.ManagementResourceRegistration;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * Resource description for the addressable resource /subsystem=infinispan/cache-container=X/cache=Y/persistence=PERSISTENCE
 *
 * @author Ryan Emerson
 */
public class PersistenceConfigurationResource extends CacheConfigurationChildResource {

   static final PathElement PATH = PathElement.pathElement(ModelKeys.PERSISTENCE, ModelKeys.PERSISTENCE_NAME);

   // attributes
   static final SimpleAttributeDefinition AVAILABILITY_INTERVAL =
         new SimpleAttributeDefinitionBuilder(ModelKeys.AVAILABILITY_INTERVAL, ModelType.INT, true)
               .setXmlName(Attribute.AVAILABILITY_INTERVAL.getLocalName())
               .setAllowExpression(false)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(PersistenceConfiguration.AVAILABILITY_INTERVAL.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition CONNECTION_ATTEMPTS =
         new SimpleAttributeDefinitionBuilder(ModelKeys.CONNECTION_ATTEMPTS, ModelType.INT, true)
               .setXmlName(Attribute.CONNECTION_ATTEMPTS.getLocalName())
               .setAllowExpression(false)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(PersistenceConfiguration.CONNECTION_ATTEMPTS.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition CONNECTION_INTERVAL =
         new SimpleAttributeDefinitionBuilder(ModelKeys.CONNECTION_INTERVAL, ModelType.INT, true)
               .setXmlName(Attribute.CONNECTION_INTERVAL.getLocalName())
               .setAllowExpression(false)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(PersistenceConfiguration.CONNECTION_INTERVAL.getDefaultValue()))
               .build();

   static final SimpleAttributeDefinition PASSIVATION =
         new SimpleAttributeDefinitionBuilder(ModelKeys.PASSIVATION, ModelType.BOOLEAN, true)
               .setXmlName(Attribute.PASSIVATION.getLocalName())
               .setAllowExpression(true)
               .setFlags(AttributeAccess.Flag.RESTART_RESOURCE_SERVICES)
               .setDefaultValue(new ModelNode().set(PersistenceConfiguration.PASSIVATION.getDefaultValue()))
               .build();

   static final AttributeDefinition[] ATTRIBUTES = {AVAILABILITY_INTERVAL, CONNECTION_ATTEMPTS, CONNECTION_INTERVAL, PASSIVATION};

   static final String[] LOADER_KEYS = new String[] { ModelKeys.LOADER, ModelKeys.CLUSTER_LOADER };
   static final String[] STORE_KEYS = new String[] { ModelKeys.STORE, ModelKeys.FILE_STORE, ModelKeys.STRING_KEYED_JDBC_STORE,
         ModelKeys.REMOTE_STORE, ModelKeys.REST_STORE, ModelKeys.ROCKSDB_STORE, ModelKeys.SOFT_INDEX_FILE_STORE};

   private CacheConfigurationResource cacheConfigResource;
   private ManagementResourceRegistration containerReg;

   PersistenceConfigurationResource(ManagementResourceRegistration containerReg, CacheConfigurationResource cacheConfigResource) {
      super(PATH, ModelKeys.PERSISTENCE, cacheConfigResource, ATTRIBUTES);
      this.cacheConfigResource = cacheConfigResource;
      this.containerReg = containerReg;
   }

   @Override
   public void registerChildren(ManagementResourceRegistration registration) {
      super.registerChildren(registration);

      registration.registerSubModel(new FileStoreResource(cacheConfigResource, containerReg));
      registration.registerSubModel(new LoaderConfigurationResource(cacheConfigResource, containerReg));
      registration.registerSubModel(new ClusterLoaderConfigurationResource(cacheConfigResource, containerReg));
      registration.registerSubModel(new RocksDBStoreConfigurationResource(cacheConfigResource, containerReg));
      registration.registerSubModel(new RemoteStoreConfigurationResource(cacheConfigResource, containerReg));
      registration.registerSubModel(new RestStoreConfigurationResource(cacheConfigResource, containerReg));
      // TODO only register if feature enabled
      registration.registerSubModel(new SoftIndexConfigurationResource(cacheConfigResource, containerReg));
      registration.registerSubModel(new StoreConfigurationResource(cacheConfigResource, containerReg));
      registration.registerSubModel(new StringKeyedJDBCStoreResource(cacheConfigResource, containerReg));
   }
}
