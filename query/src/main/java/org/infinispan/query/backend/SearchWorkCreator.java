package org.infinispan.query.backend;

import java.io.Serializable;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.spi.IndexedTypeIdentifier;

/**
 * Creates Work instances that should be performed by Hibernate-Search.
 *
 * @author Marko Luksa
 * @deprecated since 10.1.1. To be removed in version 12.0 after migrating to Hibernate Search 6.
 */
public interface SearchWorkCreator {

   /**
    * Creates a collection of Work instances that Hibernate-Search should perform for all the entities of the given
    * entity type.
    *
    * @param entityType the entity type that these Works should be created for
    * @param workType   the type of work to be done
    * @return collection of Work instances
    */
   Work createPerEntityTypeWork(IndexedTypeIdentifier entityType, WorkType workType);

   /**
    * Creates a collection of Work instances that Hibernate-Search should perform for the given entity
    *
    * @param entity   the entity that these Works should be created for
    * @param id       the id of the document
    * @param workType the type of work to be done
    * @return collection of Work instances
    */
   Work createPerEntityWork(Object entity, Serializable id, WorkType workType);

   /**
    * Creates a Work instance for a given entity.
    *
    * @param id         the id of the entity
    * @param entityType the entity type that the Work should be created for
    * @param workType   the type of the Work to be done
    * @return Work instance to be performed by the Hibernate Search engine
    */
   Work createPerEntityWork(Serializable id, IndexedTypeIdentifier entityType, WorkType workType);

   SearchWorkCreator DEFAULT = new SearchWorkCreator() {
      @Override
      public Work createPerEntityTypeWork(IndexedTypeIdentifier entityType, WorkType workType) {
         return new Work(entityType, null, workType);
      }

      @Override
      public Work createPerEntityWork(Object entity, Serializable id, WorkType workType) {
         return new Work(entity, id, workType);
      }

      @Override
      public Work createPerEntityWork(Serializable id, IndexedTypeIdentifier entityType, WorkType workType) {
         return new Work(null, entityType, id, workType);
      }
   };
}
