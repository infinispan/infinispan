package org.infinispan.query.remote.json;

import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * @since 9.4
 */
public abstract class JsonQueryResponse implements JsonSerialization {

   public static final String ENTITY_PROJECTION_KEY = "*";
   public static final String SCORE_PROJECTION_KEY = "score()";
   public static final String VERSION_PROJECTION_KEY = "version()";

   private final long totalResults;

   JsonQueryResponse(long totalResults) {
      this.totalResults = totalResults;
   }

   public long getTotalResults() {
      return totalResults;
   }
}
