package org.infinispan.search.mapper.session.impl;

import java.util.Collection;

import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;

/**
 * @author Fabio Massimo Ercoli
 */
public interface InfinispanTypeContextProvider {

   <E> InfinispanIndexedTypeContext<E> indexedForExactType(Class<E> entityType);

   InfinispanIndexedTypeContext<?> indexedForEntityName(String indexName);

   Collection<PojoRawTypeIdentifier<?>> allTypeIdentifiers();

}
