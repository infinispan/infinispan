package org.infinispan.search.mapper.mapping.impl;

import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoContainedTypeExtendedMappingCollector;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoIndexedTypeExtendedMappingCollector;
import org.hibernate.search.mapper.pojo.mapping.building.spi.PojoMapperDelegate;
import org.hibernate.search.mapper.pojo.mapping.spi.PojoMappingDelegate;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeModel;
import org.infinispan.query.concurrent.FailureCounter;
import org.infinispan.search.mapper.mapping.EntityConverter;
import org.infinispan.util.concurrent.BlockingManager;

public final class InfinispanMapperDelegate implements PojoMapperDelegate<InfinispanMappingPartialBuildState> {

   private final InfinispanTypeContextContainer.Builder typeContextContainerBuilder =
         new InfinispanTypeContextContainer.Builder();
   private final PojoSelectionEntityLoader<?> entityLoader;
   private final EntityConverter entityConverter;
   private final BlockingManager blockingManager;
   private final FailureCounter failureCounter;

   public InfinispanMapperDelegate(PojoSelectionEntityLoader<?> entityLoader, EntityConverter entityConverter,
                                   BlockingManager blockingManager, FailureCounter failureCounter) {
      this.entityLoader = entityLoader;
      this.entityConverter = entityConverter;
      this.blockingManager = blockingManager;
      this.failureCounter = failureCounter;
   }

   @Override
   public void closeOnFailure() {
      // Nothing to do
   }

   @Override
   public <E> PojoIndexedTypeExtendedMappingCollector createIndexedTypeExtendedMappingCollector(
         PojoRawTypeModel<E> rawTypeModel, String entityName) {
      return typeContextContainerBuilder.addIndexed(rawTypeModel, entityName);
   }

   @Override
   public <E> PojoContainedTypeExtendedMappingCollector createContainedTypeExtendedMappingCollector(
         PojoRawTypeModel<E> rawTypeModel, String entityName) {
      // This is a placeholder: we don't care about contained types at the moment.
      return new InfinispanContainedTypeContext.Builder();
   }

   @Override
   public InfinispanMappingPartialBuildState prepareBuild(PojoMappingDelegate mappingDelegate) {
      return new InfinispanMappingPartialBuildState(mappingDelegate, typeContextContainerBuilder.build(),
            entityLoader, entityConverter, blockingManager, failureCounter);
   }
}
