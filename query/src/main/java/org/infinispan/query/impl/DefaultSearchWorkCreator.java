package org.infinispan.query.impl;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.infinispan.query.backend.SearchWorkCreator;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

/**
 * @author Marko Luksa
 */
public class DefaultSearchWorkCreator<T> implements SearchWorkCreator<T> {

   @Override
   public Collection<Work> createPerEntityTypeWorks(Class<T> entityType, WorkType workType) {
      Work work = new Work(entityType, null, workType);
      return Collections.singleton(work);
   }

   @Override
   public Collection<Work> createPerEntityWorks(T entity, Serializable id, WorkType workType) {
      Work work = new Work(entity, id, workType);
      return Collections.singleton(work);
   }
}
