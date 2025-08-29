package org.infinispan.query.mapper.session.impl;

import java.util.Collection;

import org.hibernate.search.mapper.pojo.session.spi.PojoSearchSessionMappingContext;
import org.infinispan.query.mapper.scope.SearchScope;
import org.infinispan.query.mapper.scope.impl.SearchScopeImpl;

public interface InfinispanSearchSessionMappingContext extends PojoSearchSessionMappingContext {

   <E> SearchScopeImpl<E> createScope(Collection<? extends Class<? extends E>> classes);

   <E> SearchScope<E> createScope(Class<E> expectedSuperType, Collection<String> entityNames);
}
