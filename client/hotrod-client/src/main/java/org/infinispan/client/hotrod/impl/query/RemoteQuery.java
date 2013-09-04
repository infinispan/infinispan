package org.infinispan.client.hotrod.impl.query;

import org.infinispan.client.hotrod.impl.RemoteCacheImpl;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.impl.SortCriteria;

import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class RemoteQuery implements Query {

   private final RemoteCacheImpl cache;

   private final String jpqlString;
   private final List<SortCriteria> sortCriteria;
   private final long startOffset; //todo can this really be long or it has to be int due to limitations in query module?
   private final int maxResults;

   private List results = null;

   public RemoteQuery(RemoteCacheImpl cache, String jpqlString, List<SortCriteria> sortCriteria, long startOffset, int maxResults) {
      this.cache = cache;
      this.jpqlString = jpqlString;
      this.sortCriteria = sortCriteria;
      this.startOffset = startOffset;
      this.maxResults = maxResults;
   }

   public RemoteCacheImpl getCache() {
      return cache;
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

   public int getMaxResults() {
      return maxResults;
   }

   @Override
   public <T> List<T> list() {
      if (results == null) {
         results = cache.query(this);
      }

      return results;
   }

   @Override
   public int getResultSize() {
      return list().size();
   }
}
