package org.infinispan.search.mapper.mapping;

import org.hibernate.search.engine.backend.index.IndexManager;

/**
 * A descriptor of an indexed entity type, exposing in particular the index manager for this entity.
 */
public interface SearchIndexedEntity {

   /**
    * @return The Java class of the entity.
    */
   Class<?> javaClass();

   /**
    * @return The index manager this entity is indexed in.
    */
   IndexManager indexManager();

}
