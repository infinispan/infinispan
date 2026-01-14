package org.infinispan.query.mapper.scope.impl;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.engine.backend.work.execution.OperationSubmitter;
import org.hibernate.search.engine.backend.work.execution.spi.UnsupportedOperationBehavior;
import org.hibernate.search.mapper.pojo.work.spi.PojoScopeWorkspace;
import org.hibernate.search.util.common.impl.Futures;
import org.infinispan.query.mapper.scope.SearchWorkspace;

class SearchWorkspaceImpl implements SearchWorkspace {
   private final PojoScopeWorkspace delegate;

   public SearchWorkspaceImpl(PojoScopeWorkspace delegate) {
      this.delegate = delegate;
   }

   @Override
   public void purge() {
      Futures.unwrappedExceptionJoin(purgeAsync().toCompletableFuture());
   }

   @Override
   public CompletionStage<?> purgeAsync() {
      return delegate.purge(Collections.emptySet(), OperationSubmitter.blocking(), UnsupportedOperationBehavior.FAIL);
   }

   @Override
   public void purge(Set<String> routingKeys) {
      Futures.unwrappedExceptionJoin(delegate.purge(routingKeys, OperationSubmitter.blocking(), UnsupportedOperationBehavior.FAIL));
   }

   @Override
   public void flush() {
      Futures.unwrappedExceptionJoin(delegate.flush(OperationSubmitter.blocking(), UnsupportedOperationBehavior.FAIL));
   }

   @Override
   public void refresh() {
      Futures.unwrappedExceptionJoin(delegate.refresh(OperationSubmitter.blocking(), UnsupportedOperationBehavior.FAIL));
   }

   @Override
   public void mergeSegments() {
      Futures.unwrappedExceptionJoin(delegate.mergeSegments(OperationSubmitter.blocking(), UnsupportedOperationBehavior.FAIL));
   }
}
