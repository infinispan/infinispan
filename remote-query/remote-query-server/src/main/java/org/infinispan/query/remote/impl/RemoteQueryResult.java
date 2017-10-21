package org.infinispan.query.remote.impl;

import java.util.List;

public class RemoteQueryResult {
   private final String[] projections;
   private final int totalResults;
   private final List<Object> results;

   public RemoteQueryResult(String[] projections, int totalResults, List<Object> results) {
      this.projections = projections;
      this.totalResults = totalResults;
      this.results = results;
   }

   public String[] getProjections() {
      return projections;
   }

   public int getTotalResults() {
      return totalResults;
   }

   public List<Object> getResults() {
      return results;
   }
}
