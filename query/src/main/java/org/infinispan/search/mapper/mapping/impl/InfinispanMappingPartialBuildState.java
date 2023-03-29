package org.infinispan.search.mapper.mapping.impl;

import org.hibernate.search.engine.mapper.mapping.building.spi.MappingPartialBuildState;
import org.hibernate.search.engine.mapper.mapping.spi.MappingImplementor;
import org.hibernate.search.mapper.pojo.loading.spi.PojoSelectionEntityLoader;
import org.hibernate.search.mapper.pojo.mapping.spi.PojoMappingDelegate;
import org.infinispan.query.concurrent.FailureCounter;
import org.infinispan.search.mapper.mapping.EntityConverter;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.util.concurrent.BlockingManager;

public class InfinispanMappingPartialBuildState implements MappingPartialBuildState {

   private final PojoMappingDelegate mappingDelegate;
   private final InfinispanTypeContextContainer typeContextContainer;
   private final PojoSelectionEntityLoader<?> entityLoader;
   private final EntityConverter entityConverter;
   private final BlockingManager blockingManager;
   private final FailureCounter failureCounter;

   InfinispanMappingPartialBuildState(PojoMappingDelegate mappingDelegate,
                                      InfinispanTypeContextContainer typeContextContainer,
                                      PojoSelectionEntityLoader<?> entityLoader,
                                      EntityConverter entityConverter,
                                      BlockingManager blockingManager,
                                      FailureCounter failureCounter) {
      this.mappingDelegate = mappingDelegate;
      this.typeContextContainer = typeContextContainer;
      this.entityLoader = entityLoader;
      this.entityConverter = entityConverter;
      this.blockingManager = blockingManager;
      this.failureCounter = failureCounter;
   }

   @Override
   public void closeOnFailure() {
      mappingDelegate.close();
   }

   public MappingImplementor<SearchMapping> finalizeMapping() {
      return new InfinispanMapping(mappingDelegate, typeContextContainer, entityLoader, entityConverter,
            blockingManager, failureCounter);
   }
}
