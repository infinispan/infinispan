package org.infinispan.query.remote.client;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class QueryRequest {

   private String jpqlString;

   private List<SortCriteria> sortCriteria;

   private long startOffset;

   private int maxResults;

   public String getJpqlString() {
      return jpqlString;
   }

   public void setJpqlString(String jpqlString) {
      this.jpqlString = jpqlString;
   }

   public List<SortCriteria> getSortCriteria() {
      return sortCriteria;
   }

   public void setSortCriteria(List<SortCriteria> sortCriteria) {
      this.sortCriteria = sortCriteria;
   }

   public long getStartOffset() {
      return startOffset;
   }

   public void setStartOffset(long startOffset) {
      this.startOffset = startOffset;
   }

   public int getMaxResults() {
      return maxResults;
   }

   public void setMaxResults(int maxResults) {
      this.maxResults = maxResults;
   }

   public static final class SortCriteria {

      private String attributePath;

      private boolean isAscending;

      public String getAttributePath() {
         return attributePath;
      }

      public void setAttributePath(String attributePath) {
         this.attributePath = attributePath;
      }

      public boolean isAscending() {
         return isAscending;
      }

      public void setAscending(boolean ascending) {
         isAscending = ascending;
      }
   }
}
