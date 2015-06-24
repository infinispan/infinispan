package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.SortOrder;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public final class SortCriteria {

   private final Expression pathExpression;

   private final SortOrder sortOrder;

   SortCriteria(Expression pathExpression, SortOrder sortOrder) {
      if (pathExpression == null) {
         throw new IllegalArgumentException("pathExpression cannot be null");
      }
      if (sortOrder == null) {
         throw new IllegalArgumentException("sortOrder cannot be null");
      }
      this.pathExpression = pathExpression;
      this.sortOrder = sortOrder;
   }

   public Expression getAttributePath() {
      return pathExpression;
   }

   public SortOrder getSortOrder() {
      return sortOrder;
   }

   @Override
   public String toString() {
      return "SortCriteria{" +
            "pathExpression='" + pathExpression + '\'' +
            ", sortOrder=" + sortOrder +
            '}';
   }
}
