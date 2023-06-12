package org.infinispan.query.remote.impl;

import java.util.List;

public final class RemoteQueryResult {
   private final String[] projections;
   private final int hitCount;
   private final boolean hitCountExact;
   private final List<Object> results;

   RemoteQueryResult(String[] projections, int hitCount, boolean hitCountExact, List<Object> results) {
      this.projections = projections;
      this.hitCount = hitCount;
      this.hitCountExact = hitCountExact;
      this.results = results;
   }

   public String[] getProjections() {
      return projections;
   }

   public int hitCount() {
      return hitCount;
   }

   public boolean hitCountExact() {
      return hitCountExact;
   }

   public List<Object> getResults() {
      return results;
   }
}
