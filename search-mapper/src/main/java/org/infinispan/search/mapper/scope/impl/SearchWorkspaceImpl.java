package org.infinispan.search.mapper.scope.impl;

import java.util.Collections;
import java.util.Set;

import org.hibernate.search.mapper.pojo.work.spi.PojoScopeWorkspace;
import org.hibernate.search.util.common.impl.Futures;
import org.infinispan.search.mapper.scope.SearchWorkspace;

class SearchWorkspaceImpl implements SearchWorkspace {
   private final PojoScopeWorkspace delegate;

   public SearchWorkspaceImpl(PojoScopeWorkspace delegate) {
      this.delegate = delegate;
   }

   @Override
   public void purge() {
      Futures.unwrappedExceptionJoin(delegate.purge(Collections.emptySet()));
   }

   @Override
   public void purge(Set<String> routingKeys) {
      Futures.unwrappedExceptionJoin(delegate.purge(routingKeys));
   }

   @Override
   public void flush() {
      Futures.unwrappedExceptionJoin(delegate.flush());
   }

   @Override
   public void refresh() {
      Futures.unwrappedExceptionJoin(delegate.refresh());
   }

   @Override
   public void mergeSegments() {
      Futures.unwrappedExceptionJoin(delegate.mergeSegments());
   }
}
