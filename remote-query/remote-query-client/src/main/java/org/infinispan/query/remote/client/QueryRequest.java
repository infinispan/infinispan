package org.infinispan.query.remote.client;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class QueryRequest {

   private String jpqlString;

   private long startOffset;

   private int maxResults;

   public String getJpqlString() {
      return jpqlString;
   }

   public void setJpqlString(String jpqlString) {
      this.jpqlString = jpqlString;
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
}
