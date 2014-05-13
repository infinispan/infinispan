package org.infinispan.objectfilter.query;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.SortCriteria;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class FilterQuery implements Query {

   private String jpqlString;
   private List<SortCriteria> sortCriteria;
   private long startOffset;
   private int maxResults;

   public FilterQuery(String jpqlString, List<SortCriteria> sortCriteria, long startOffset, int maxResults) {
      this.jpqlString = jpqlString;
      this.sortCriteria = sortCriteria;
      this.startOffset = startOffset;
      this.maxResults = maxResults;
   }

   // TODO [anistor] need to rethink the dsl Query/QueryBuilder interfaces to accommodate the filtering scenario ...
   @Override
   public <T> List<T> list() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int getResultSize() {
      throw new UnsupportedOperationException();
   }

   public String getJpqlString() {
      return jpqlString;
   }

   public List<SortCriteria> getSortCriteria() {
      return sortCriteria;
   }

   public long getStartOffset() {
      return startOffset;
   }
}
