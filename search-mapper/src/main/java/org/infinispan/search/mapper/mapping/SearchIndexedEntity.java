/*
 * Hibernate Search, full-text search for your domain model
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
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
