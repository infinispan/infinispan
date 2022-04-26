package org.infinispan.search.mapper.session.impl;

import org.hibernate.search.mapper.pojo.identity.spi.IdentifierMapping;
import org.hibernate.search.mapper.pojo.model.spi.PojoRawTypeIdentifier;

/**
 * @param <E> The entity type mapped to the index.
 *
 * @author Fabio Massimo Ercoli
 */
public interface InfinispanIndexedTypeContext<E> {

   PojoRawTypeIdentifier<E> typeIdentifier();

   String name();

   IdentifierMapping identifierMapping();

}
