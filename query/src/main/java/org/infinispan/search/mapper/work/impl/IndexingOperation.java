package org.infinispan.search.mapper.work.impl;

import java.util.concurrent.CompletableFuture;

import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.infinispan.util.concurrent.NonBlockingManager;

public abstract class IndexingOperation extends CompletableFuture<Void> {

   protected final PojoIndexer delegate;
   protected final Object providedId;
   protected final String routingKey;

   public IndexingOperation(PojoIndexer delegate, Object providedId, String routingKey) {
      this.delegate = delegate;
      this.providedId = providedId;
      this.routingKey = routingKey;
   }

   final void invoke(PojoIndexer pojoIndexer, NonBlockingManager nonBlockingManager) {
      invoke(pojoIndexer)
            .whenComplete((v, t) -> {
               if (t != null) {
                  nonBlockingManager.completeExceptionally(this, t);
               } else {
                  nonBlockingManager.complete(this, null);
               }
            });
   }

   abstract CompletableFuture<?> invoke(PojoIndexer pojoIndexer);

}
