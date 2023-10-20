package org.infinispan.objectfilter.impl.syntax.parser;

import java.util.Collections;

import org.infinispan.objectfilter.impl.ql.AggregationFunction;
import org.infinispan.objectfilter.impl.ql.PropertyPath;
import org.infinispan.objectfilter.impl.syntax.parser.projection.CacheValuePropertyPath;

public class CacheValueAggregationPropertyPath<TypeDescriptor> extends AggregationPropertyPath<TypeDescriptor> {

   CacheValueAggregationPropertyPath() {
      super(AggregationFunction.COUNT, Collections.singletonList(
            new PropertyPath.PropertyReference<>(CacheValuePropertyPath.VALUE_PROPERTY_NAME, null, true)));
   }
}
