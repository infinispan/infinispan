package org.infinispan.query.remote.impl;

import java.util.List;

public class RemoteQueryResult {
   private final String[] projections;
   private final long totalResults;
   private final List<Object> results;

   RemoteQueryResult(String[] projections, long totalResults, List<Object> results) {
      this.projections = projections;
      this.totalResults = totalResults;
      this.results = results;
   }

   public String[] getProjections() {
      return projections;
   }

   public long getTotalResults() {
      return totalResults;
   }

   public List<Object> getResults() {
      return results;
   }
}
