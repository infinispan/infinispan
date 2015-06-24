package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Expression;

/**
 * Represents the path of a field, including the aggregation function if any.
 *
 * @author anistor@redhat.com
 * @since 8.0
 */
public final class PathExpression implements Expression {

   public enum AggregationType {
      SUM, AVG, MIN, MAX, COUNT
   }

   /**
    * Optional aggregation type.
    */
   private final AggregationType aggregationType;

   private final String path;

   public PathExpression(AggregationType aggregationType, String path) {
      this.aggregationType = aggregationType;
      this.path = path;
   }

   public AggregationType getAggregationType() {
      return aggregationType;
   }

   public String getPath() {
      return path;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || o.getClass() != PathExpression.class) return false;
      PathExpression that = (PathExpression) o;
      return aggregationType == that.aggregationType && path.equals(that.path);
   }

   @Override
   public int hashCode() {
      return 31 * (aggregationType != null ? aggregationType.hashCode() : 0) + path.hashCode();
   }

   @Override
   public String toString() {
      return aggregationType != null ? aggregationType.name() + '(' + path + ')' : path;
   }
}
