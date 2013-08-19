package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.SortOrder;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class SortCriteria {

   private final String attributePath;

   private final SortOrder sortOrder;

   SortCriteria(String attributePath, SortOrder sortOrder) {
      this.attributePath = attributePath;
      this.sortOrder = sortOrder;
   }

   public String getAttributePath() {
      return attributePath;
   }

   public SortOrder getSortOrder() {
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
