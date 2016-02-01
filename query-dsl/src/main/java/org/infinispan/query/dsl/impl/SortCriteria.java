package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;
import org.infinispan.query.dsl.SortOrder;
import org.infinispan.query.dsl.impl.logging.Log;
import org.jboss.logging.Logger;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
final class SortCriteria {

   private static final Log log = Logger.getMessageLogger(Log.class, SortCriteria.class.getName());

   private final Expression pathExpression;

   private final SortOrder sortOrder;

   SortCriteria(Expression pathExpression, SortOrder sortOrder) {
      if (pathExpression == null) {
         throw log.argumentCannotBeNull("pathExpression");
      }
      if (sortOrder == null) {
         throw log.argumentCannotBeNull("sortOrder");
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
