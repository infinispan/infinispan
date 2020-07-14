package org.infinispan.query.remote.json;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * @since 9.4
 */
public abstract class JsonQueryResponse implements JsonSerialization {

   private final long totalResults;

   JsonQueryResponse(long totalResults) {
      this.totalResults = totalResults;
   }

   public long getTotalResults() {
      return totalResults;
   }
}
