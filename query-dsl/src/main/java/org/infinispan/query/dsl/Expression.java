package org.infinispan.query.dsl;

import org.infinispan.query.dsl.impl.ParameterExpression;
import org.infinispan.query.dsl.impl.PathExpression;

/**
 * @author anistor@redhat.com
 * @since 8.0
 * @deprecated since 10.1. See deprecation note on {@link QueryBuilder}.
 */
@Deprecated
public interface Expression {

   static Expression param(String paramName) {
      return new ParameterExpression(paramName);
   }

   static Expression property(String attributePath) {
      return new PathExpression(null, attributePath);
   }

   static Expression count(String attributePath) {
      return new PathExpression(PathExpression.AggregationType.COUNT, attributePath);
   }

   static Expression sum(String attributePath) {
      return new PathExpression(PathExpression.AggregationType.SUM, attributePath);
   }

   static Expression avg(String attributePath) {
      return new PathExpression(PathExpression.AggregationType.AVG, attributePath);
   }

   static Expression min(String attributePath) {
      return new PathExpression(PathExpression.AggregationType.MIN, attributePath);
   }

   static Expression max(String attributePath) {
      return new PathExpression(PathExpression.AggregationType.MAX, attributePath);
   }
}
