package org.infinispan.query.objectfilter.impl.syntax.parser;

import java.lang.invoke.MethodHandles;
import java.util.List;

import org.infinispan.query.objectfilter.impl.logging.Log;
import org.infinispan.query.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.query.objectfilter.impl.ql.PropertyPath;
import org.infinispan.query.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;
import org.jboss.logging.Logger;

/**
 * An aggregated property path (e.g. {@code SUM(foo.bar.baz)}) represented by {@link PropertyReference}s
 * used with an aggregation function in the SELECT, HAVING or ORDER BY clause.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public class AggregationPropertyPath<TypeMetadata> extends PropertyPath<TypeDescriptor<TypeMetadata>> {

   private static final Log log = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, AggregationPropertyPath.class.getName());

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
            throw log.aggregationTypeNotSupportedException(aggregationFunction.name());
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
      String stringPath = asStringPath();
      if (CacheValuePropertyPath.VALUE_PROPERTY_NAME.equals(stringPath)) {
         stringPath = "*";
      }
      return aggregationFunction.name() + "(" + stringPath + ")";
   }
}
