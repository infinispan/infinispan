package org.infinispan.query.mapper.impl;

import java.util.Collection;

import org.hibernate.search.engine.mapper.mapping.building.spi.MappingBuildContext;
import org.hibernate.search.engine.mapper.mapping.building.spi.MappingConfigurationCollector;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoMapperDelegate;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoTypeMetadataContributor;
import org.hibernate.search.mapper.pojo.mapping.spi.AbstractPojoMappingInitiator;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.infinispan.query.concurrent.FailureCounter;
import org.infinispan.query.impl.IndexerConfig;
import org.infinispan.query.impl.EntityLoaderFactory;
import org.infinispan.query.mapper.mapping.EntityConverter;
import org.infinispan.query.mapper.mapping.MappingConfigurationContext;
import org.infinispan.query.mapper.mapping.ProgrammaticSearchMappingProvider;
import org.infinispan.query.mapper.mapping.impl.InfinispanMapperDelegate;
import org.infinispan.query.mapper.mapping.impl.InfinispanMappingPartialBuildState;
import org.infinispan.util.concurrent.BlockingManager;

public class InfinispanMappingInitiator extends AbstractPojoMappingInitiator<InfinispanMappingPartialBuildState>
   implements MappingConfigurationContext {

   private final InfinispanTypeConfigurationContributor typeConfigurationContributor;
   private final Collection<ProgrammaticSearchMappingProvider> mappingProviders;
   private final BlockingManager blockingManager;
   private final FailureCounter failureCounter;
   private final IndexerConfig indexerConfig;

   private EntityLoaderFactory<?> entityLoader;
   private EntityConverter entityConverter;

   public InfinispanMappingInitiator(PojoBootstrapIntrospector introspector,
                                     Collection<ProgrammaticSearchMappingProvider> mappingProviders,
                                     BlockingManager blockingManager, FailureCounter failureCounter,
                                     IndexerConfig indexerConfig) {
      super(introspector, InfinispanSearchMapperHints.INSTANCE);
      typeConfigurationContributor = new InfinispanTypeConfigurationContributor(introspector);
      this.indexerConfig = indexerConfig;
      addConfigurationContributor(typeConfigurationContributor);
      this.mappingProviders = mappingProviders;
      this.blockingManager = blockingManager;
      this.failureCounter = failureCounter;
   }

   public void addEntityType(Class<?> type, String entityName) {
      typeConfigurationContributor.addEntityType(type, entityName);
   }

   public void setEntityLoader(EntityLoaderFactory<?> entityLoader) {
      this.entityLoader = entityLoader;
   }

   public void setEntityConverter(EntityConverter entityConverter) {
      this.entityConverter = entityConverter;
   }

   @Override
   public void configure(MappingBuildContext buildContext,
                         MappingConfigurationCollector<PojoTypeMetadataContributor> configurationCollector) {
      for (ProgrammaticSearchMappingProvider mappingProvider : mappingProviders) {
         mappingProvider.configure(this);
      }
      super.configure(buildContext, configurationCollector);
   }

   @Override
   protected PojoMapperDelegate<InfinispanMappingPartialBuildState> createMapperDelegate() {
      return new InfinispanMapperDelegate(entityLoader, entityConverter, blockingManager, failureCounter, indexerConfig);
   }
}
