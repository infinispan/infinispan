package org.infinispan.query.objectfilter.impl.syntax.parser;

import java.util.List;
import java.util.Objects;

import org.infinispan.query.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.query.objectfilter.impl.ql.Function;

/**
 * An aggregation applied to a function property path (e.g. {@code MAX(distance(location, lat, lon))}).
 *
 * @since 16.2
 */
public class AggregationFunctionPropertyPath<TypeMetadata> extends AggregationPropertyPath<TypeMetadata> {

   private final Function function;
   private final List<Object> args;

   AggregationFunctionPropertyPath(AggregationFunction aggregationFunction,
                                   List<PropertyReference<TypeDescriptor<TypeMetadata>>> path,
                                   Function function, List<Object> args) {
      super(aggregationFunction, path);
      this.function = function;
      this.args = args;
   }

   public Function getInnerFunction() {
      return function;
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;
      AggregationFunctionPropertyPath<?> that = (AggregationFunctionPropertyPath<?>) o;
      return function == that.function && Objects.equals(args, that.args);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), function, args);
   }

   public List<Object> getInnerArgs() {
      return args;
   }

   @Override
   public String toString() {
      return "AggregationFunctionPropertyPath{" +
            "function=" + function +
            ", args=" + args +
            ", aggregationFunction=" + super.toString() +
            '}';
   }

   public FunctionPropertyPath<TypeMetadata> toFunctionPropertyPath() {
      return new FunctionPropertyPath<>(getNodes(), function, args);
   }

}
