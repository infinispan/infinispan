package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.List;

import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.jboss.logging.Logger;

/**
 * An aggregated property path (e.g. {@code SUM(foo.bar.baz)}) represented by {@link PropertyReference}s
 * used with an aggregation function in the SELECT, HAVING or ORDER BY clause.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class AggregationPropertyPath<TypeMetadata> extends PropertyPath<TypeDescriptor<TypeMetadata>> {

   private static final Log log = Logger.getMessageLogger(Log.class, AggregationPropertyPath.class.getName());

   private final AggregationFunction aggregationFunction;

   AggregationPropertyPath(AggregationFunction aggregationFunction, List<PropertyReference<TypeDescriptor<TypeMetadata>>> path) {
      super(path);
      switch (aggregationFunction) {
         case SUM:
         case AVG:
         case MIN:
         case MAX:
         case COUNT:
            break;
         default:
            throw log.getAggregationTypeNotSupportedException(aggregationFunction.name());
      }
      this.aggregationFunction = aggregationFunction;
   }

   public AggregationFunction getAggregationFunction() {
      return aggregationFunction;
   }

   @Override
   public boolean equals(Object o) {
      if (!super.equals(o)) return false;
      AggregationPropertyPath<?> that = (AggregationPropertyPath<?>) o;
      return aggregationFunction == that.aggregationFunction;
   }

   @Override
   public int hashCode() {
      return 31 * super.hashCode() + aggregationFunction.hashCode();
   }

   @Override
   public String toString() {
      return aggregationFunction.name() + "(" + asStringPath() + ")";
   }
}
