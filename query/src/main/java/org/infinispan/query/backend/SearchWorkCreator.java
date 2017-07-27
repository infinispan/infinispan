package org.infinispan.query.backend;

import java.io.Serializable;
import java.util.Collection;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.spi.IndexedTypeIdentifier;

/**
 * Creates collections of Work instances that should be performed by Hibernate-Search.
 *
 * @author Marko Luksa
 */
public interface SearchWorkCreator {

   /**
    * Creates a collection of Work instances that Hibernate-Search should perform for all the entities of the given
    * entity type.
    * @param entityType the entity type that these Works should be created for
    * @param workType the type of work to be done
    * @return collection of Work instances
    */
   Collection<Work> createPerEntityTypeWorks(IndexedTypeIdentifier entityType, WorkType workType);

   /**
    * Creates a collection of Work instances that Hibernate-Search should perform for the given entity
    * @param entity the entity that these Works should be created for
    * @param id the id of the document
    * @param workType the type of work to be done
    * @return collection of Work instances
    */
   Collection<Work> createPerEntityWorks(Object entity, Serializable id, WorkType workType);

   /**
    * Creates a Work instance for a given entity.
    *
    * @param id the id of the entity
    * @param entityType the entity type that the Work should be created for
    * @param workType the type of the Work to be done
    * @return Work instance to be performed by the Hibernate Search engine
    */
   Work createEntityWork(Serializable id, IndexedTypeIdentifier entityType, WorkType workType);

}
