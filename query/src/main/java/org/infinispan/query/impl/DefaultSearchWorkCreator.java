package org.infinispan.query.impl;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.infinispan.query.backend.SearchWorkCreator;

/**
 * @author Marko Luksa
 */
public class DefaultSearchWorkCreator implements SearchWorkCreator {

   @Override
   public Collection<Work> createPerEntityTypeWorks(IndexedTypeIdentifier entityType, WorkType workType) {
      Work work = new Work(entityType, null, workType);
      return Collections.singleton(work);
   }

   @Override
   public Collection<Work> createPerEntityWorks(Object entity, Serializable id, WorkType workType) {
      Work work = new Work(entity, id, workType);
      return Collections.singleton(work);
   }

   @Override
   public Work createEntityWork(Serializable id, IndexedTypeIdentifier entityType, WorkType workType) {
      return new Work(null, entityType, id, workType);
   }
}
