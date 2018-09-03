package org.infinispan.query.impl;

import java.util.List;

import org.hibernate.search.query.engine.spi.EntityInfo;

import net.jcip.annotations.NotThreadSafe;

/**
 * This is the implementation class for the interface ResultIterator. It is what is
 * returned when the {@link org.infinispan.query.CacheQuery#iterator()} using
 * a {@link org.infinispan.query.FetchOptions.FetchMode#EAGER}.
 * <p/>
 *
 * @author Navin Surtani
 * @author Marko Luksa
 */
@NotThreadSafe
final class EagerIterator<E> extends AbstractIterator<E> {

   private final List<EntityInfo> entityInfos;

   EagerIterator(List<EntityInfo> entityInfos, QueryResultLoader resultLoader, int fetchSize) {
      super(resultLoader, 0, entityInfos.size() - 1, fetchSize);
      this.entityInfos = entityInfos;
   }

   @Override
   public void close() {
      // This method does not need to do anything for this type of iterator as when an instance of it is
      // created, the iterator() method in CacheQueryImpl closes everything that needs to be closed.
   }

   @Override
   protected EntityInfo loadEntityInfo(int index) {
      return entityInfos.get(index);
   }
}
