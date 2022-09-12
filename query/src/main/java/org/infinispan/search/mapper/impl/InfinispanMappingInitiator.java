package org.infinispan.search.mapper.impl;

import java.util.Collection;

import org.hibernate.search.engine.mapper.mapping.building.spi.MappingBuildContext;
import org.hibernate.search.engine.mapper.mapping.building.spi.MappingConfigurationCollector;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoMapperDelegate;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoTypeMetadataContributor;
import org.hibernate.search.mapper.pojo.mapping.spi.AbstractPojoMappingInitiator;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.infinispan.search.mapper.mapping.EntityConverter;
import org.infinispan.search.mapper.mapping.MappingConfigurationContext;
import org.infinispan.search.mapper.mapping.ProgrammaticSearchMappingProvider;
import org.infinispan.search.mapper.mapping.impl.InfinispanMapperDelegate;
import org.infinispan.search.mapper.mapping.impl.InfinispanMappingPartialBuildState;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.NonBlockingManager;

public class InfinispanMappingInitiator extends AbstractPojoMappingInitiator<InfinispanMappingPartialBuildState>
   implements MappingConfigurationContext {

   private final InfinispanTypeConfigurationContributor typeConfigurationContributor;
   private final Collection<ProgrammaticSearchMappingProvider> mappingProviders;

   private PojoSelectionEntityLoader<?> entityLoader;
   private EntityConverter entityConverter;
   private BlockingManager blockingManager;
   private NonBlockingManager nonBlockingManager;

   public InfinispanMappingInitiator(PojoBootstrapIntrospector introspector,
                                     Collection<ProgrammaticSearchMappingProvider> mappingProviders,
                                     BlockingManager blockingManager, NonBlockingManager nonBlockingManager) {
      super(introspector);
      typeConfigurationContributor = new InfinispanTypeConfigurationContributor(introspector);
      addConfigurationContributor(typeConfigurationContributor);
      this.mappingProviders = mappingProviders;
      this.blockingManager = blockingManager;
      this.nonBlockingManager = nonBlockingManager;
   }

   public void addEntityType(Class<?> type, String entityName) {
      typeConfigurationContributor.addEntityType(type, entityName);
   }

   public void setEntityLoader(PojoSelectionEntityLoader<?> entityLoader) {
      this.entityLoader = entityLoader;
   }

   public void setEntityConverter(EntityConverter entityConverter) {
      this.entityConverter = entityConverter;
   }

   @Override
   public void configure(MappingBuildContext buildContext,
                         MappingConfigurationCollector<PojoTypeMetadataContributor> configurationCollector) {
      super.configure(buildContext, configurationCollector);

      for (ProgrammaticSearchMappingProvider mappingProvider : mappingProviders) {
         mappingProvider.configure(this);
      }
   }

   @Override
   protected PojoMapperDelegate<InfinispanMappingPartialBuildState> createMapperDelegate() {
      return new InfinispanMapperDelegate(entityLoader, entityConverter, blockingManager, nonBlockingManager);
   }
}
