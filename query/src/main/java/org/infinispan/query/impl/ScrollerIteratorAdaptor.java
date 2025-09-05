package org.infinispan.query.impl;

import java.util.List;
import java.util.NoSuchElementException;

import org.hibernate.search.engine.search.query.SearchScroll;
import org.hibernate.search.engine.search.query.SearchScrollResult;
import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.HitCount;
import org.infinispan.commons.query.TotalHitCount;

/**
 * Adaptor to use a link {@link SearchScroll} as an iterator.
 *
 * @since 12.0
 */
public class ScrollerIteratorAdaptor<E> implements ClosableIteratorWithCount<E> {
   private final SearchScroll<E> scroll;
   private SearchScrollResult<E> scrollResult;
   private List<E> chunk;
   private int cursor = 0;

   public ScrollerIteratorAdaptor(SearchScroll<E> scroll) {
      this.scroll = scroll;
      this.scrollResult = scroll.next();
      this.chunk = scrollResult.hits();
   }

   @Override
   public boolean hasNext() {
      tryFetchMore();
      return scrollResult.hasHits();
   }

   @Override
   public E next() {
      if (hasNext()) {
         return chunk.get(cursor++);
      }
      throw new NoSuchElementException();
   }

   private void tryFetchMore() {
      if (cursor == chunk.size()) {
         scrollResult = scroll.next();
         chunk = scrollResult.hits();
         cursor = 0;
      }
   }

   @Override
   public void close() {
      scroll.close();
   }

   @Override
   public HitCount count() {
      return new TotalHitCount((int) scrollResult.total().hitCountLowerBound(), scrollResult.total().isHitCountExact());
   }
}
