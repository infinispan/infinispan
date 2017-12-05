package org.infinispan.query.dsl.embedded.impl;

import org.apache.lucene.search.Sort;
import org.hibernate.search.query.engine.spi.HSQuery;

/**
 * Stores the definition of a {@link HSQuery}.
 *
 * @since 9.2
 */
public class HsQueryRequest {

   private final HSQuery hsQuery;
   private final Sort sort;
   private final String[] projections;

   HsQueryRequest(HSQuery hsQuery, Sort sort, String[] projections) {
      this.hsQuery = hsQuery;
      this.sort = sort;
      this.projections = projections;
   }

   public HSQuery getHsQuery() {
      return hsQuery;
   }

   public Sort getSort() {
      return sort;
   }

   public String[] getProjections() {
      return projections;
   }
}
