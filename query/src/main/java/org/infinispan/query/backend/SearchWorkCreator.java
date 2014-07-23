package org.infinispan.query.backend;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;

import java.io.Serializable;
import java.util.Collection;

/**
 * Creates collections of Work instances that should be performed by Hibernate-Search.
 *
 * @author Marko Luksa
 */
public interface SearchWorkCreator<T> {

   /**
    * Creates a collection of Work instances that Hibernate-Search should perform for all the entities of the given
    * entity type.
    * @param entityType the entity type that these Works should be created for
    * @param workType the type of work to be done
    * @return collection of Work instances
    */
   Collection<Work> createPerEntityTypeWorks(Class<T> entityType, WorkType workType);

   /**
    * Creates a collection of Work instances that Hibernate-Search should perform for the given entity
    * @param entity the entity that these Works should be created for
    * @param id the id of the document
    * @param workType the type of work to be done
    * @return collection of Work instances
    */
   Collection<Work> createPerEntityWorks(T entity, Serializable id, WorkType workType);
}
