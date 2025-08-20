package org.infinispan.query.mapper.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.hibernate.search.engine.mapper.mapping.building.spi.MappingBuildContext;
import org.hibernate.search.engine.mapper.mapping.building.spi.MappingConfigurationCollector;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoTypeMetadataContributor;
import org.hibernate.search.mapper.pojo.mapping.spi.PojoMappingConfigurationContext;
import org.hibernate.search.mapper.pojo.mapping.spi.PojoMappingConfigurationContributor;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.infinispan.query.mapper.log.impl.Log;
import org.infinispan.util.logging.LogFactory;

class InfinispanTypeConfigurationContributor implements PojoMappingConfigurationContributor {

   private static final Log log = LogFactory.getLog(InfinispanTypeConfigurationContributor.class, Log.class);

   private final PojoBootstrapIntrospector introspector;

   // Use a LinkedHashMap for deterministic iteration
   private final Map<String, Class<?>> entityTypeByName = new LinkedHashMap<>();

   public InfinispanTypeConfigurationContributor(PojoBootstrapIntrospector introspector) {
      this.introspector = introspector;
   }

   @Override
   public void configure(MappingBuildContext buildContext, PojoMappingConfigurationContext configurationContext,
           MappingConfigurationCollector<PojoTypeMetadataContributor> configurationCollector) {
      for (Map.Entry<String, Class<?>> entry : entityTypeByName.entrySet()) {
         PojoRawTypeModel<?> typeModel = identifiedByName(entry.getValue()) ? introspector.typeModel(entry.getKey())
               : introspector.typeModel(entry.getValue());
         configurationCollector.collectContributor(typeModel,
               new InfinispanEntityTypeContributor(typeModel.typeIdentifier(), entry.getKey()));
      }
   }

   void addEntityType(Class<?> type, String entityName) {
      Class<?> previousType = entityTypeByName.putIfAbsent(entityName, type);
      if (previousType != null && previousType != type) {
         throw log.multipleEntityTypesWithSameName(entityName, previousType, type);
      }
   }

   private boolean identifiedByName(Class<?> type) {
      return byte[].class == type;
   }

}
