package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.SortOrder;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class SortCriteria {

   private final String attributePath;

   private final SortOrder sortOrder;

   SortCriteria(String attributePath, SortOrder sortOrder) {
      this.attributePath = attributePath;
      this.sortOrder = sortOrder;
   }

   String getAttributePath() {
      return attributePath;
   }

   SortOrder getSortOrder() {
      return sortOrder;
   }

   @Override
   public String toString() {
      return "SortCriteria{" +
            "attributePath='" + attributePath + '\'' +
            ", sortOrder=" + sortOrder +
            '}';
   }
}
