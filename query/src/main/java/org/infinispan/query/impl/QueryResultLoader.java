package org.infinispan.query.impl;

import org.hibernate.search.engine.search.loading.spi.EntityLoader;

import org.infinispan.search.mapper.common.EntityReference;

/**
 * @param <E> The entity type mapped to the index.
 *
 * @author <a href="mailto:mluksa@redhat.com">Marko Luksa</a>
 */
public interface QueryResultLoader<E> extends EntityLoader<EntityReference, E> {

   E loadBlocking(EntityReference entityInfo);
}
