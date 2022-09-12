package org.infinispan.search.mapper.work.impl;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.engine.backend.work.execution.DocumentCommitStrategy;
import org.hibernate.search.engine.backend.work.execution.DocumentRefreshStrategy;
import org.hibernate.search.mapper.pojo.route.DocumentRoutesDescriptor;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.infinispan.search.mapper.session.impl.InfinispanTypeContextProvider;

public class PurgeIndexingOperation extends IndexingOperation {

   private final InfinispanTypeContextProvider typeContextProvider;

   PurgeIndexingOperation(InfinispanTypeContextProvider typeContextProvider, PojoIndexer delegate, Object providedId,
                          String routingKey) {
      super(delegate, providedId, routingKey);
      this.typeContextProvider = typeContextProvider;
   }

   @Override
   CompletableFuture<Void> invoke(PojoIndexer pojoIndexer) {
      return CompletableFuture.allOf(typeContextProvider.allTypeIdentifiers().stream()
            .map((typeIdentifier) -> delegate.delete(typeIdentifier, providedId,
                  DocumentRoutesDescriptor.fromLegacyRoutingKey(routingKey),
                  DocumentCommitStrategy.NONE, DocumentRefreshStrategy.NONE))
            .toArray(CompletableFuture[]::new));
   }
}
