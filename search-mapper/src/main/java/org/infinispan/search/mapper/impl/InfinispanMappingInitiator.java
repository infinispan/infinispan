package org.infinispan.search.mapper.impl;

import java.util.Collection;

import org.hibernate.search.engine.mapper.mapping.building.spi.MappingBuildContext;
import org.hibernate.search.engine.mapper.mapping.building.spi.MappingConfigurationCollector;
import org.hibernate.search.engine.search.loading.spi.EntityLoader;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoMapperDelegate;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoTypeMetadataContributor;
import org.hibernate.search.mapper.pojo.mapping.spi.AbstractPojoMappingInitiator;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.infinispan.search.mapper.common.EntityReference;
import org.infinispan.search.mapper.mapping.EntityConverter;
import org.infinispan.search.mapper.mapping.MappingConfigurationContext;
import org.infinispan.search.mapper.mapping.ProgrammaticSearchMappingProvider;
import org.infinispan.search.mapper.mapping.impl.InfinispanMapperDelegate;
import org.infinispan.search.mapper.mapping.impl.InfinispanMappingPartialBuildState;

public class InfinispanMappingInitiator extends AbstractPojoMappingInitiator<InfinispanMappingPartialBuildState>
   implements MappingConfigurationContext {

   private final InfinispanTypeConfigurationContributor typeConfigurationContributor;
   private final EntityLoader<EntityReference, ?> entityLoader;
   private final Collection<ProgrammaticSearchMappingProvider> mappingProviders;

   private EntityConverter entityConverter;

   public InfinispanMappingInitiator(PojoBootstrapIntrospector introspector, EntityLoader<EntityReference, ?> entityLoader,
                                     Collection<ProgrammaticSearchMappingProvider> mappingProviders) {
      super(introspector);
      typeConfigurationContributor = new InfinispanTypeConfigurationContributor(introspector);
      addConfigurationContributor(typeConfigurationContributor);
      this.entityLoader = entityLoader;
      this.mappingProviders = mappingProviders;
   }

   public void addEntityType(Class<?> type, String entityName) {
      typeConfigurationContributor.addEntityType(type, entityName);
   }

   public void setEntityConverter(EntityConverter entityConverter) {
      this.entityConverter = entityConverter;
   }

   @Override
   public void configure(MappingBuildContext buildContext, MappingConfigurationCollector<PojoTypeMetadataContributor> configurationCollector) {
      super.configure(buildContext, configurationCollector);

      for (ProgrammaticSearchMappingProvider mappingProvider : mappingProviders) {
         mappingProvider.configure(this);
      }
   }

   @Override
   protected PojoMapperDelegate<InfinispanMappingPartialBuildState> createMapperDelegate() {
      return new InfinispanMapperDelegate(entityLoader, entityConverter);
   }
}
