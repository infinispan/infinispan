package org.infinispan.search.mapper.work.impl;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.engine.backend.work.execution.DocumentCommitStrategy;
import org.hibernate.search.engine.backend.work.execution.DocumentRefreshStrategy;
import org.hibernate.search.mapper.pojo.route.DocumentRoutesDescriptor;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;

public class AddOrUpdateIndexingOperation extends IndexingOperation {

   private final SearchIndexerImpl.ConvertedValue convertedValue;

   AddOrUpdateIndexingOperation(PojoIndexer delegate, Object providedId, String routingKey,
                                SearchIndexerImpl.ConvertedValue convertedValue) {
      super(delegate, providedId, routingKey);
      this.convertedValue = convertedValue;
   }

   @Override
   CompletableFuture<?> invoke(PojoIndexer pojoIndexer) {
      return delegate.addOrUpdate(convertedValue.typeIdentifier, providedId,
            DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey), convertedValue.value,
            DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE);
   }
}
