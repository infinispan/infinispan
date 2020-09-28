package org.infinispan.search.mapper.session.impl;

import org.hibernate.search.mapper.pojo.bridge.runtime.spi.IdentifierMapping;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;

/**
 * @param <E> The entity type mapped to the index.
 *
 * @author Fabio Massimo Ercoli
 */
public interface InfinispanIndexedTypeContext<E> {

   PojoRawTypeIdentifier<E> getTypeIdentifier();

   String getEntityName();

   IdentifierMapping getIdentifierMapping();

}
